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
    int lambda = 5;
    if (argc == 2) {
        lambda = atoi(argv[1]);
    }
    unordered_map<uint64_t, uint64_t> map;
    exponential_distribution distribution(lambda, 100);

    for (uint64_t count = 0; count < 10000; ++count) {
        uint64_t val = distribution.next();
        map[val]++;
    }

//    for (int count = 0; count < 100; ++count) {
//        string val(map[count] / 200, '*');
//        cout << count << ": " << val << endl;
//    }

    std::random_device rd;
    std::mt19937 gen;
    gen.seed(rd());

    // if particles decay once per second on average,
    // how much time, in seconds, until the next one?
    std::exponential_distribution<> d(1.0);

    std::map<int, int> hist;
    for(int n=0; n<10000; ++n) {
        ++hist[3*d(gen)];
    }
    for(auto p : hist) {
        std::cout << std::fixed << std::setprecision(1)
        << p.first/3.0 << '-' << (p.first+1)/3.0 <<
        ' ' << std::string(p.second/300, '*') << '\n';
    }

    return 0;
}