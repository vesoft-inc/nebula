/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <folly/Benchmark.h>
#include "common/base/ICord.h"
#include "common/base/Cord.h"

using nebula::ICord;
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
BENCHMARK_RELATIVE(icord_10k_string, iters) {
    for (auto i = 0u; i < iters; i++) {
        ICord<> cord;
        for (int j = 0; j < 1000; j++) {
            cord << "abcdefghij";
        }
        std::string str = cord.str();
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
BENCHMARK_RELATIVE(icord_1k_mix, iters) {
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
BENCHMARK_RELATIVE(icord_512_mix, iters) {
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
// In Intel(R) Xeon(R) Platinum 8260M CPU @ 2.30GHz
============================================================================
/root/nebula/src/common/base/test/ICordBenchmark.cpprelative  time/iter  iters/s
============================================================================
sstream_10k_string                                          12.53us   79.81K
icord_10k_string                                 348.85%     3.59us  278.41K
cord_10k_string                                  181.23%     6.91us  144.63K
----------------------------------------------------------------------------
sstream_1k_mix                                              15.13us   66.09K
icord_1k_mix                                      54.54%    27.74us   36.05K
cord_1k_mix                                       53.59%    28.24us   35.42K
----------------------------------------------------------------------------
sstream_512_mix                                              7.77us  128.68K
icord_512_mix                                     55.98%    13.88us   72.04K
cord_512_mix                                      55.08%    14.11us   70.89K
============================================================================
*/


// In summary, compare to cord improve about 2-3 x performance
// and avoid dynamic allocation when write a little data
