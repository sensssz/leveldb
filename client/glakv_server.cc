#include "config.h"
#include "exponential_distribution.h"

#include <getopt.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <signal.h>

#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>
#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <fcntl.h>

#define BUF_LEN     (VAL_LEN * 2)
#define GET         "Get"
#define PUT         "Put"
#define DEL         "Del"
#define QUIT        "Quit"
#define DB_SIZE     1000000
#define NUM_CLIENTS 128
#define NUM_EXP     100000
#define INT_LEN     (sizeof(uint64_t) / sizeof(char))

using std::cerr;
using std::cout;
using std::endl;
using std::min;
using std::ostream;
using std::rand;
using std::string;
using std::mutex;
using std::thread;
using std::chrono::time_point;
using std::uniform_int_distribution;
using std::vector;
using leveldb::Cache;
using leveldb::DB;
using leveldb::Options;
using leveldb::Status;
using leveldb::Slice;
using leveldb::WriteOptions;
using leveldb::ReadOptions;
using leveldb::WriteBatch;
using leveldb::NewLRUCache;

static bool quit = false;
static vector<thread> threads;
static mutex lock;

void quit_server(int) {
    cout << "Receives CTRL-C, quiting..." << endl;
    quit = true;
}

void error(const char *msg)
{
    perror(msg);
    exit(1);
}

void usage(ostream &os) {
    os << "Usage: glakv [OPTIONS]" << endl;
    os << "[OPTIONS]:" << endl;
    os << "--help" << endl;
    os << "-h       show this message" << endl;
    os << "--dir" << endl;
    os << "-d       directory to store the database files" << endl;
}

static inline DB *db_open(string &dir, Cache *cache, bool create) {
    DB *db;
    Options options;
    options.create_if_missing = create;
    options.block_cache = cache;
    Status status = DB::Open(options, dir, &db);
    if (!status.ok()) {
        cerr << status.ToString() << endl;
        exit(1);
    }
    return db;
}

void parse_opts(int argc, char *argv[], int &help_flag, string &dir) {
    struct option long_options[] = {
            {"help",    no_argument,       0, 'h'},
            {"dir",     required_argument, 0, 'd'},
            {0, 0, 0, 0}
    };

    int c;
    int option_index;
    help_flag = 0;
    dir = "glakv_home";
    while ((c = getopt_long(argc, argv, "hd:", long_options, &option_index)) != -1) {
        switch(c) {
            case 'h':
                help_flag = 1;
                break;
            case 'd':
                dir = optarg;
                break;
            case '?':
                break;
            default:
                usage(cerr);
                break;
        }
    }
}

int setup_server() {
    int sockfd, portno;
    struct sockaddr_in serv_addr;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        error("ERROR opening socket");
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    portno = 4242;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);
    if (bind(sockfd, (struct sockaddr *) &serv_addr,
             sizeof(serv_addr)) < 0) {
        error("ERROR on binding");
    }
    return sockfd;
}

void store_uint64(char *buf, uint64_t val) {
    *((uint64_t *) buf) = val;
}

uint64_t get_unit64(char *buf) {
    return *((uint64_t *) buf);
}

void serve_client(int sockfd, DB *db, vector<double> &latencies) {
    char buffer[BUF_LEN];
    char res[BUF_LEN];
    uint64_t res_len = 0;
    ssize_t len = 0;
    while (!quit) {
        bzero(buffer, BUF_LEN);
        if ((len = read(sockfd, buffer, BUF_LEN)) < 0) {
            cerr << "Error reading from client" << endl;
            break;
        }
        int GET_LEN = strlen(GET);
        int PUT_LEN = strlen(PUT);
        int DEL_LEN = strlen(DEL);
        int QUIT_LEN = strlen(QUIT);
        time_point<std::chrono::high_resolution_clock> start;
        time_point<std::chrono::high_resolution_clock> end;
        if (strncmp(GET, buffer, GET_LEN) == 0) {
            uint64_t klen = get_unit64(buffer + GET_LEN);
//            assert(len == GET_LEN + INT_LEN + klen);
            Slice key(buffer + GET_LEN + INT_LEN, klen);
            string val;
            start = std::chrono::high_resolution_clock::now();
            Status s = db->Get(ReadOptions(), key, &val);
            end = std::chrono::high_resolution_clock::now();
            if (s.ok()) {
                res[0] = 1;
                store_uint64(res + 1, val.size());
                memcpy(res + 1 + INT_LEN, val.c_str(), val.size());
                res_len = 1 + INT_LEN + val.size();
            } else {
                res[0] = 0;
                res_len = 1;
            }
        } else if (strncmp(PUT, buffer, PUT_LEN) == 0) {
            uint64_t klen = get_unit64(buffer + PUT_LEN);
            uint64_t vlen = get_unit64(buffer + PUT_LEN + INT_LEN + klen);
            if (len != PUT_LEN + INT_LEN + klen + INT_LEN + vlen) {
                cout << len << "," << PUT_LEN + INT_LEN + klen + INT_LEN + vlen << endl;
            }
//            assert(len == PUT_LEN + INT_LEN + klen + INT_LEN + vlen);
            char *key_buf = buffer + PUT_LEN + INT_LEN;
            char *val_buf = buffer + PUT_LEN + INT_LEN + klen + INT_LEN;
            Slice key(key_buf, klen);
            Slice val(val_buf, vlen);
            start = std::chrono::high_resolution_clock::now();
            Status s = db->Put(WriteOptions(), key, val);
            end = std::chrono::high_resolution_clock::now();
            if (s.ok()) {
                res[0] = 1;
                res_len = 1;
            } else {
                res[0] = 0;
                res_len = 1;
            }
        } else if (strncmp(DEL, buffer, DEL_LEN) == 0) {
            uint64_t klen = get_unit64(buffer + DEL_LEN);
//            assert(len == DEL_LEN + INT_LEN + klen);
            char *key_buf = buffer + strlen(DEL) + INT_LEN;
            Slice key(key_buf, klen);
            start = std::chrono::high_resolution_clock::now();
            Status s = db->Delete(WriteOptions(), key);
            end = std::chrono::high_resolution_clock::now();
            if (s.ok()) {
                res[0] = 1;
                res_len = 1;
            } else {
                res[0] = 0;
                res_len = 1;
            }
        } else if (strncmp(QUIT, buffer, QUIT_LEN) == 0) {
            break;
        }
        auto diff = end - start;
        lock.lock();
        latencies.push_back(diff.count());
        lock.unlock();
        if (write(sockfd, res, res_len) != res_len) {
            cerr << "Error sending result to client" << endl;
            break;
        }
    }
    close(sockfd);
}

int main(int argc, char *argv[])
{
    signal(SIGINT, quit_server);

    int help_flag = 0;
    string dir;
    parse_opts(argc, argv, help_flag, dir);

    if (help_flag) {
        usage(cout);
        return 0;
    }

    DB *db = db_open(dir, nullptr, true);
    int sockfd = setup_server();
    int newsockfd;
    socklen_t clilen;
    struct sockaddr_in cli_addr;
    listen(sockfd, 5);
    clilen = sizeof(cli_addr);
    int flags = fcntl(sockfd, F_GETFL, 0);
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
    vector<double> latencies;
    mutex lock;
    auto start = std::chrono::high_resolution_clock::now();
    while (!quit) {
        newsockfd = accept(sockfd,
                           (struct sockaddr *) &cli_addr,
                           &clilen);
        if (newsockfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(100);
                continue;
            } else {
                error("ERROR on accept");
            }
        }
        flags = fcntl(newsockfd, F_GETFL, 0);
        fcntl(newsockfd, F_SETFL, flags & ~O_NONBLOCK);
        thread t(serve_client, newsockfd, db, latencies);
        threads.push_back(std::move(t));
    }

    for (auto &t : threads) {
        t.join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto diff = end - start;

    double sum = 0;
    for (auto latency : latencies) {
        sum += latency;
    }

    cout << "Mean latency: " << (sum / latencies.size()) << endl;
    cout << "Throughput: " << latencies.size() / diff.count() << endl;

    close(sockfd);
    return 0;
}