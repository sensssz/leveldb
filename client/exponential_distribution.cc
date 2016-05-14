//
// Created by Jiamin Huang on 5/13/16.
//

#include "exponential_distribution.h"

using std::ceil;

static const double LN10 = 2.30258509299;
static const int PRECISION = 4;

exponential_distribution::exponential_distribution(double lambda_in, uint64_t size_in) : lambda(lambda_in), dist(1 / lambda),
                                                                                    size(size_in) {
    std::random_device rd;
    generator.seed(rd());
    max_val = PRECISION / lambda * LN10;
    interval_size = max_val / size;
}

uint64_t exponential_distribution::next() {
    uint64_t key = 0;
    double rand;
    do {
        rand = dist(generator);
        key = (uint64_t) (rand / interval_size);
    } while (rand >= max_val);
    return key;
}

