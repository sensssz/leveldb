#include "config.h"
#include "exponential_distribution.h"

#include <getopt.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <signal.h>

#include <atomic>
#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>
#include <future>
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

using std::atomic;
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
static bool prefetch = false;
static vector<thread> threads;
static atomic<int> num_threads(0);
static bool reported = true;

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
            {"prefetch",no_argument,       0, 'p'},
            {"dir",     required_argument, 0, 'd'},
            {0, 0, 0, 0}
    };

    int c;
    int option_index;
    help_flag = 0;
    dir = "glakv_home";
    while ((c = getopt_long(argc, argv, "hpd:", long_options, &option_index)) != -1) {
        switch(c) {
            case 'h':
                help_flag = 1;
                break;
            case 'p':
                prefetch = true;
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
    int yes = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
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

static inline uint64_t *id_field(char *key, int klen) {
    return (uint64_t *) (key + (klen * sizeof(char) - sizeof(uint64_t)) / sizeof(char));
}

void prefetch_kv(DB* db, Slice key) {
    if (prefetch) {
        string val;
        db->Get(ReadOptions(), key, &val);
    }
}

void prefetch_for_key(DB *db, char *key_buf, int klen) {
    uint64_t *id = id_field(key_buf, klen);
    for (int count = 0; count < NUM_PREFETCH; ++count) {
        *id += 1;
        Slice key(key_buf, klen);
        std::async(prefetch_kv, db, key);
    }
}

void serve_client(int sockfd, DB *db, vector<double> &latencies, mutex &lock) {
    reported = false;
    char buffer[BUF_LEN];
    char res[BUF_LEN];
    uint64_t res_len = 0;
    ssize_t len = 0;
    bool is_get = false;
    char *key_buf;
    uint64_t klen;
    while (!quit) {
        is_get = false;
        bzero(buffer, BUF_LEN);
        if ((len = read(sockfd, buffer, BUF_LEN)) < 0) {
            cerr << "Error reading from client" << endl;
            break;
        }
        int GET_LEN = strlen(GET);
        int PUT_LEN = strlen(PUT);
        int DEL_LEN = strlen(DEL);
        int QUIT_LEN = strlen(QUIT);
        std::chrono::duration<double> diff;
        if (strncmp(GET, buffer, GET_LEN) == 0) {
            is_get = true;
            klen = get_unit64(buffer + GET_LEN);
//            assert(len == GET_LEN + INT_LEN + klen);
            key_buf = buffer + GET_LEN + INT_LEN;
            Slice key(key_buf, klen);
            string val;
            auto start = std::chrono::high_resolution_clock::now();
            Status s = db->Get(ReadOptions(), key, &val);
            auto end = std::chrono::high_resolution_clock::now();
            diff = end - start;
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
            klen = get_unit64(buffer + PUT_LEN);
            uint64_t vlen = get_unit64(buffer + PUT_LEN + INT_LEN + klen);
            if (len != PUT_LEN + INT_LEN + klen + INT_LEN + vlen) {
                cout << len << "," << PUT_LEN + INT_LEN + klen + INT_LEN + vlen << endl;
            }
//            assert(len == PUT_LEN + INT_LEN + klen + INT_LEN + vlen);
            char *key_buf = buffer + PUT_LEN + INT_LEN;
            char *val_buf = buffer + PUT_LEN + INT_LEN + klen + INT_LEN;
            Slice key(key_buf, klen);
            Slice val(val_buf, vlen);
            auto start = std::chrono::high_resolution_clock::now();
            Status s = db->Put(WriteOptions(), key, val);
            auto end = std::chrono::high_resolution_clock::now();
            diff = end - start;
            if (s.ok()) {
                res[0] = 1;
                res_len = 1;
            } else {
                res[0] = 0;
                res_len = 1;
            }
        } else if (strncmp(DEL, buffer, DEL_LEN) == 0) {
            klen = get_unit64(buffer + DEL_LEN);
//            assert(len == DEL_LEN + INT_LEN + klen);
            char *key_buf = buffer + strlen(DEL) + INT_LEN;
            Slice key(key_buf, klen);
            auto start = std::chrono::high_resolution_clock::now();
            Status s = db->Delete(WriteOptions(), key);
            auto end = std::chrono::high_resolution_clock::now();
            diff = end - start;
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
        if (is_get) {
            prefetch_for_key(db, key_buf, klen);
        }
        if (write(sockfd, res, res_len) != res_len) {
            cerr << "Error sending result to client" << endl;
            break;
        }
        lock.lock();
        latencies.push_back(diff.count());
        lock.unlock();
    }
    --num_threads;
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
    while (!quit) {
        newsockfd = accept(sockfd,
                           (struct sockaddr *) &cli_addr,
                           &clilen);
        if (newsockfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (num_threads == 0 && !reported) {
                    double sum = 0;
                    for (auto latency : latencies) {
                        sum += latency;
                    }
                    cout << (sum / latencies.size()) << endl;
                    latencies.clear();
                    reported = true;
                }
                usleep(100);
                continue;
            } else {
                error("ERROR on accept");
            }
        }
        flags = fcntl(newsockfd, F_GETFL, 0);
        fcntl(newsockfd, F_SETFL, flags & ~O_NONBLOCK);
        thread t(serve_client, newsockfd, db, std::ref(latencies), std::ref(lock));
        threads.push_back(std::move(t));
        ++num_threads;
    }

    for (auto &t : threads) {
        t.join();
    }

    close(sockfd);
    delete db;
    return 0;
}