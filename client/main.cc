//
// Created by Jiamin Huang on 5/13/16.
//

#include "config.h"

#include <getopt.h>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#define DB_SIZE 512

using std::cout;
using std::cerr;
using std::endl;
using std::ostream;
using leveldb::DB;
using leveldb::Options;
using leveldb::Status;
using leveldb::Slice;
using leveldb::WriteOptions;
using leveldb::ReadOptions;
using leveldb::WriteBatch;

void load_data(DB *db) {
    WriteBatch batch;
    for (int count = 0; count < DB_SIZE; ++count) {
        char key_buf[KEY_LEN];
        char val_buf[VAL_LEN];
        Slice key(key_buf, KEY_LEN);
        Slice val(val_buf, VAL_LEN);
        batch.Put(key, val);
    }
    db->Write(WriteOptions(), &batch);
}

void usage(ostream &os) {
    os << "Usage: glakv -l load database" << endl;
    os << "             -e execute workload" << endl;
    os << "             -h print this message" << endl;
}

int main(int argc, char *argv[]) {
    DB *db;
    Options options;
    options.create_if_missing = true;
    Status status = DB::Open(options, "/tmp/testdb", &db);
    if (!status.ok()) {
        cerr << status.ToString() << endl;
    }
    int c;
    while ((c = getopt(argc, argv, "leh")) != -1) {
        switch(c) {
            case 'l':
                load_data(db);
                break;
            case 'e':
                break;
            case 'h':
                usage(cout);
                break;
            default:
                usage(cerr);
        }
    }
    return 0;
}
