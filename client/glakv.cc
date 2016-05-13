//
// Created by Jiamin Huang on 5/13/16.
//

#include "config.h"
#include "exponential_distribution.h"

#include <getopt.h>
#include <iostream>
#include <thread>
#include <leveldb/cache.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#define DB_SIZE     1000000
#define NUM_CLIENTS 128
#define NUM_EXP     100000

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

static inline uint64_t *id_field(char *key, int klen) {
    return (uint64_t *) (key + (klen * sizeof(char) - sizeof(uint64_t)) / sizeof(char));
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

void load_data(string &dir, int db_size) {
    DB *db = db_open(dir, nullptr, true);
    cout << "Loading " << db_size << " kv pairs into the database..." << endl;
    char key_buf[KEY_LEN];
    bzero(key_buf, KEY_LEN);
    uint64_t *id = id_field(key_buf, KEY_LEN);

    // Load data into the database, one batch at a time
    WriteBatch batch;
    WriteOptions options;
    options.sync = true;
    const int batch_size = db_size / 5;
    uint64_t count = 0;
    while (count < db_size) {
        int num_writes = min(batch_size, (const int &) (db_size - count));
        for (uint64_t i = 0; i < num_writes; ++i, ++count) {
            char val_buf[VAL_LEN];
            *id = count;
            Slice key(key_buf, KEY_LEN);
            Slice val(val_buf, VAL_LEN);
            batch.Put(key, val);
        }
        Status status = db->Write(options, &batch);
        if (!status.ok()) {
            cerr << status.ToString() << endl;
            break;
        }
        batch.Clear();
        uint64_t percentage_done = (count * 100) / db_size;
        string progress_bar(percentage_done, '.');
        cout << "Loading" << progress_bar << percentage_done << '%' << endl;
    }
    cout << "All kv pairs loaded into the database." << endl;
    delete db;
}

/*
 * Keys in the database is from 0 to DB_SIZE - 1.
 * Whenever a key is read, the next key to be read follow
 * an exponential distribution, where key + 1 has the highest
 * possibility, key + 2 has the second highest possibility, etc.
 */
void execute(DB *db, int database_size, int num_exps) {
    exponential_distribution exp_dist(5, database_size);
    char key_buf[KEY_LEN];
    bzero(key_buf, KEY_LEN);
    uint64_t *id = id_field(key_buf, KEY_LEN);
    uint64_t key = (uint64_t) (rand() % database_size);
    string val;
    for (int count = 0; count < num_exps; ++count) {
        *id = key;
        Slice key_slice(key_buf, KEY_LEN);
        Status s = db->Get(ReadOptions(), key_slice, &val);
        assert(s.ok());
        uint64_t next_rank = exp_dist.next();
        key = (next_rank + key) % database_size;
    }
}

void run(string &dir, int num_threads, int database_size, int num_exps) {
    Cache *cache = NewLRUCache(10 * 1024 * 1024);
    DB *db = db_open(dir, cache, false);
    vector<thread> threads;
    for (int count = 0; count < num_threads; ++count) {
        thread t(execute, db, database_size, num_exps);
        threads.push_back(std::move(t));
    }
    for (auto &t : threads) {
        t.join();
    }

    delete db;
    delete cache;
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
    os << "--dir" << endl;
    os << "-d       directory to store the database files" << endl;
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
            {"dir",     required_argument, 0, 'd'},
            {0, 0, 0, 0}
    };

    int c;
    int option_index;
    int load_flag = 0;
    int execute_flag = 0;
    int help_flag = 0;
    int database_size = DB_SIZE;
    int num_clients = NUM_CLIENTS;
    int num_exps = NUM_EXP;
    string dir("glakv_home");
    while ((c = getopt_long(argc, argv, "lehs:c:d:n:", long_options, &option_index)) != -1) {
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
                database_size = atoi(optarg);
            case 'c':
                num_clients = atoi(optarg);
            case 'd':
                dir = optarg;
            case 'n':
                num_exps = atoi(optarg);
            case '?':
                break;
            default:
                usage(cerr);
        }
    }

    if (help_flag || (!load_flag && !execute_flag)) {
        usage(cout);
        return 0;
    }

    if (load_flag) {
        load_data(dir, database_size);
    }

    if (execute_flag) {
        run(dir, num_clients, database_size, num_exps);
    }
    return 0;
}
