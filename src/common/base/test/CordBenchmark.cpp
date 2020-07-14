/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "base/Cord.h"

using nebula::Cord;

BENCHMARK(sstream_10k_string, iters) {
    for (auto i = 0u; i < iters; i++) {
        std::stringstream ss;
        for (int j = 0; j < 1000; j++) {
            ss << "abcdefghij";
        }
        std::string str = ss.str();
        folly::doNotOptimizeAway(&str);
    }
}
BENCHMARK_RELATIVE(cord_10k_string, iters) {
    for (auto i = 0u; i < iters; i++) {
        Cord cord;
        for (int j = 0; j < 1000; j++) {
            cord << "abcdefghij";
        }
        std::string str = cord.str();
        folly::doNotOptimizeAway(&str);
    }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(sstream_1k_mix, iters) {
    for (auto i = 0u; i < iters; i++) {
        std::stringstream ss;
        for (int j = 0; j < 50; j++) {
            ss << "abcdefg" << 1234567890L << true << 1.23456789;
        }
        std::string str = ss.str();
        folly::doNotOptimizeAway(&str);
    }
}
BENCHMARK_RELATIVE(cord_1k_mix, iters) {
    for (auto i = 0u; i < iters; i++) {
        Cord cord;
        for (int j = 0; j < 50; j++) {
            cord << "abcdefg"
                 << folly::to<std::string>(1234567890L)
                 << folly::to<std::string>(true)
                 << folly::to<std::string>(1.23456789);
        }
        std::string str = cord.str();
        folly::doNotOptimizeAway(&str);
    }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(sstream_512_mix, iters) {
    for (auto i = 0u; i < iters; i++) {
        std::stringstream ss;
        for (int j = 0; j < 25; j++) {
            ss << "abcdefg" << 1234567890L << true << 1.23456789;
        }
        std::string str = ss.str();
        folly::doNotOptimizeAway(&str);
    }
}
BENCHMARK_RELATIVE(cord_512_mix, iters) {
    for (auto i = 0u; i < iters; i++) {
        Cord cord;
        for (int j = 0; j < 25; j++) {
            cord << "abcdefg"
                 << folly::to<std::string>(1234567890L)
                 << folly::to<std::string>(true)
                 << folly::to<std::string>(1.23456789);
        }
        std::string str = cord.str();
        folly::doNotOptimizeAway(&str);
    }
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    folly::runBenchmarks();
    return 0;
}


/*
Benchmark number is from virtualbox running on i7-8650
============================================================================
CordBenchmark.cpp                               relative  time/iter  iters/s
============================================================================
----------------------------------------------------------------------------
sstream_10k_string                                          44.75us   22.35K
cord_10k_string                                  137.72%    32.49us   30.78K
----------------------------------------------------------------------------
sstream_1k_mix                                              32.51us   30.76K
cord_1k_mix                                      207.38%    15.68us   63.79K
----------------------------------------------------------------------------
============================================================================
*/

