/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "base/ICord.h"

using nebula::ICord;

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
        ICord<> cord;
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
        ICord<> cord;
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
        ICord<> cord;
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
// In Intel(R) Xeon(R) Platinum 8260M CPU @ 2.30GHz
============================================================================
/root/nebula/src/common/base/test/ICordBenchmark.cpprelative  time/iter  iters/s
============================================================================
sstream_10k_string                                          10.65us   93.88K
cord_10k_string                                  394.79%     2.70us  370.62K
----------------------------------------------------------------------------
sstream_1k_mix                                              14.64us   68.31K
cord_1k_mix                                       52.16%    28.07us   35.63K
----------------------------------------------------------------------------
sstream_512_mix                                              7.51us  133.08K
cord_512_mix                                      51.81%    14.50us   68.95K
============================================================================
*/


// In summary, compare to cord improve about 2-3 x performance
// and avoid dynamic allocation when write a little data
