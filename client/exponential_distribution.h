//
// Created by Jiamin Huang on 5/13/16.
//

#ifndef LEVELDB_EXPONENTIAL_DISTRIBUTION_H
#define LEVELDB_EXPONENTIAL_DISTRIBUTION_H

#include <random>

class exponential_distribution {
private:
    std::exponential_distribution<> dist;
    std::mt19937 generator;
    double lambda;
    int size;
    double max_val;
    double interval_size;

public:
    exponential_distribution(double lambda_in, int size_in);

    uint64_t next();
};


#endif //LEVELDB_EXPONENTIAL_DISTRIBUTION_H
