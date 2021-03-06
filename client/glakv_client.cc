//
// Created by Jiamin Huang on 5/13/16.
//

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
#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

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
using std::thread;
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

static int lambda = 1;

static inline uint64_t *id_field(char *key, int klen) {
    return (uint64_t *) (key + klen - INT_LEN);
}

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

static int connect() {
    int sockfd;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        error("ERROR opening socket");
    }
    server = gethostbyname("salat3.eecs.umich.edu");
    if (server == NULL) {
        error("ERROR, no such host");
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr,
          (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(4242);
    if (connect(sockfd, (struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) {
        error("ERROR connecting");
    }
    return sockfd;
}

void store_uint64(char *buf, uint64_t val) {
    *((uint64_t *) buf) = val;
}

uint64_t get_unit64(char *buf) {
    return *((uint64_t *) buf);
}

Slice send_get(int sockfd, const char *key_buf, int klen) {
    char cmd_buf[BUF_LEN];
    char res_buf[BUF_LEN];
    int GET_LEN = strlen(GET);
    memcpy(cmd_buf, GET, GET_LEN);
    store_uint64(cmd_buf + GET_LEN, klen);
    memcpy(cmd_buf + GET_LEN + INT_LEN, key_buf, klen);
    int len = GET_LEN + INT_LEN + klen;
    if (write(sockfd, cmd_buf, len) != len) {
        error("ERROR sending command");
    }
    if (read(sockfd, res_buf, BUF_LEN) < 0) {
        error("ERROR receiving result");
    }
    assert(res_buf[0] == 1);
    uint64_t vlen = get_unit64(res_buf + 1);
    Slice val(res_buf + 1 + INT_LEN, vlen);
    return val;
}

void send_put(int sockfd, const char *key_buf, int klen, const char *val_buf, int vlen) {
    char cmd_buf[BUF_LEN];
    char res_buf[BUF_LEN];
    int PUT_LEN = strlen(PUT);
    memcpy(cmd_buf, PUT, PUT_LEN);
    store_uint64(cmd_buf + PUT_LEN, klen);
    assert(get_unit64(cmd_buf + PUT_LEN) == klen);
    memcpy(cmd_buf + PUT_LEN + INT_LEN, key_buf, klen);
    store_uint64(cmd_buf + PUT_LEN + INT_LEN + klen, vlen);
    memcpy(cmd_buf + PUT_LEN + INT_LEN + klen + INT_LEN, val_buf, vlen);
    int len = PUT_LEN + INT_LEN + klen + INT_LEN + vlen;
    if (write(sockfd, cmd_buf, len) != len) {
        error("ERROR sending command");
    }
    if (read(sockfd, res_buf, BUF_LEN) < 0) {
        error("ERROR receiving result");
    }
    assert(res_buf[0] == 1);
}

void send_quit(int sockfd) {
    write(sockfd, QUIT, strlen(QUIT));
    close(sockfd);
}

void load_data(uint64_t db_size) {
    int sockfd = connect();
    cout << "Loading " << db_size << " kv pairs into the database..." << endl;
    char key_buf[KEY_LEN];
    bzero(key_buf, KEY_LEN);
    uint64_t *id = id_field(key_buf, KEY_LEN);

    // Load data into the database, one batch at a time
    const uint64_t batch_size = db_size / 5;
    uint64_t count = 0;
    while (count < db_size) {
        uint64_t num_writes = min(batch_size, db_size - count);
        for (uint64_t i = 0; i < num_writes; ++i, ++count) {
            char val_buf[VAL_LEN];
            *id = count;
            send_put(sockfd, key_buf, KEY_LEN, val_buf, VAL_LEN);
        }
        uint64_t percentage_done = (count * 100) / db_size;
        string progress_bar(percentage_done, '.');
        cout << "Loading" << progress_bar << percentage_done << '%' << endl;
    }
    cout << "All kv pairs loaded into the database." << endl;
    send_quit(sockfd);
}

/*
 * Keys in the database is from 0 to DB_SIZE - 1.
 * Whenever a key is read, the next key to be read follow
 * an exponential distribution, where key + 1 has the highest
 * possibility, key + 2 has the second highest possibility, etc.
 */
void execute(uint64_t database_size, int num_exps) {
    int sockfd = connect();
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<uint64_t> uni_dist(0, database_size - 1);
    exponential_distribution exp_dist(lambda, database_size);
    char key_buf[KEY_LEN];
    bzero(key_buf, KEY_LEN);
    uint64_t *id = id_field(key_buf, KEY_LEN);
    uint64_t key = (uint64_t) (rand() % database_size);
    for (int count = 0; count < num_exps; ++count) {
        *id = key;
        send_get(sockfd, key_buf, KEY_LEN);
        if (count % 10 == 0) {
            key = uni_dist(generator);
        } else {
            uint64_t next_rank = exp_dist.next();
            key = (next_rank + key + database_size / 3) % database_size;
        }
    }
    send_quit(sockfd);
    close(sockfd);
}

void run(int num_threads, uint64_t database_size, int num_exps) {
    vector<thread> threads;
    for (int count = 0; count < num_threads; ++count) {
        thread t(execute, database_size, num_exps);
        threads.push_back(std::move(t));
    }
    for (auto &t : threads) {
        t.join();
    }
}

void usage(ostream &os) {
    os << "Usage: glakv [OPTIONS]" << endl;
    os << "[OPTIONS]:" << endl;
    os << "--load" << endl;
    os << "-l       load data into database" << endl;
    os << "--execute" << endl;
    os << "-e       execute benchmark" << endl;
    os << "--help" << endl;
    os << "-h       show this message" << endl;
    os << "--size" << endl;
    os << "-s       number of kv pairs in/to load into the database" << endl;
    os << "--client" << endl;
    os << "-c       number of concurrent clients" << endl;
    os << "--lambda" << endl;
    os << "-m       parameter for exponential distribution" << endl;
    os << "--num" << endl;
    os << "-n       number of operations each client does" << endl;
}

int main(int argc, char *argv[]) {
    struct option long_options[] = {
            {"load",    no_argument,       0, 'l'},
            {"execute", no_argument,       0, 'e'},
            {"help",    no_argument,       0, 'h'},
            {"size",    required_argument, 0, 's'},
            {"client",  required_argument, 0, 'c'},
            {"num",     required_argument, 0, 'n'},
            {"lambda",  required_argument, 0, 'm'},
            {0, 0, 0, 0}
    };

    int c;
    int option_index;
    int load_flag = 0;
    int execute_flag = 0;
    int help_flag = 0;
    uint64_t database_size = DB_SIZE;
    int num_clients = NUM_CLIENTS;
    int num_exps = NUM_EXP;
    while ((c = getopt_long(argc, argv, "lehs:c:m:n:", long_options, &option_index)) != -1) {
        switch(c) {
            case 'l':
                load_flag = 1;
                break;
            case 'e':
                execute_flag = 1;
                break;
            case 'h':
                help_flag = 1;
                break;
            case 's':
                database_size = strtoull(optarg, nullptr, 10);
                break;
            case 'c':
                num_clients = atoi(optarg);
                break;
            case 'm':
                lambda = atoi(optarg);
            case 'n':
                num_exps = atoi(optarg);
                break;
            case '?':
                break;
            default:
                usage(cerr);
                break;
        }
    }

    if (help_flag || (!load_flag && !execute_flag)) {
        usage(cout);
        return 0;
    }

    if (load_flag) {
        load_data(database_size);
    }

    if (execute_flag) {
        auto start = std::chrono::high_resolution_clock::now();
        run(num_clients, database_size, num_exps);
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end-start;
        cout << "Throughput: " << (num_clients * num_exps) / diff.count() << endl;
    }
    return 0;
}
