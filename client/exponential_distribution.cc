//
// Created by Jiamin Huang on 5/13/16.
//

#include "exponential_distribution.h"

#include <iostream>

using std::cout;
using std::endl;

using std::ceil;

static const double LN10 = 2.30258509299;
static const int PRECISION = 4;

exponential_distribution::exponential_distribution(double lambda_in, uint64_t size_in) : lambda(lambda_in), dist(1.0 / lambda_in),
                                                                                    size(size_in) {
    std::random_device rd;
    generator.seed(rd());
    max_val = PRECISION / lambda * LN10;
    interval_size = max_val / size;
    cout << "max val is " << max_val << endl;
    cout << "interval size is " << interval_size << endl;
}

uint64_t exponential_distribution::next() {
    uint64_t key = 0;
    double rand;
    do {
        rand = dist(generator);
        key = (uint64_t) (rand / 0.0921034);
    } while (rand >= max_val);
    return key;
}

