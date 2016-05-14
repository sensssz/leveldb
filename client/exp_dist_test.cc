//
// Created by Jiamin Huang on 5/14/16.
//

#include "exponential_distribution.h"

#include <iostream>
#include <string>
#include <unordered_map>
#include <iomanip>
#include <map>

using std::cout;
using std::endl;
using std::string;
using std::unordered_map;

int main(int argc, char *argv[]) {
    double lambda = 5;
    if (argc == 2) {
        lambda = atof(argv[1]);
    }
    unordered_map<uint64_t, uint64_t> map;
    exponential_distribution distribution(lambda, 100);

    for (uint64_t count = 0; count < 10000; ++count) {
        uint64_t val = distribution.next();
        map[val]++;
    }

    for (int count = 0; count < 100; ++count) {
        string val(map[count] / 100, '*');
        cout << count << ": " << val << endl;
    }

    return 0;
}