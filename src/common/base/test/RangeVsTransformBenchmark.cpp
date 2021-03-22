/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <folly/init/Init.h>
#include <folly/Benchmark.h>
#include <algorithm>
#include <vector>


BENCHMARK(Test1_RangeTestStr) {
    std::vector<int32_t> from;
    std::vector<std::string> to;
    BENCHMARK_SUSPEND {
        from.resize(1000, 0);
        to.resize(1000);
    }
    int32_t index = 0;
    for (auto& i : from) {
        to[index++] = std::to_string(i);
    }
    folly::doNotOptimizeAway(to);
}
BENCHMARK_RELATIVE(Test1_TransformStr) {
    std::vector<int32_t> from;
    std::vector<std::string> to;
    BENCHMARK_SUSPEND {
        from.resize(1000, 0);
        to.resize(1000);
    }
    std::transform(from.begin(), from.end(), to.begin(), [] (const auto& e) {
        return std::to_string(e);
    });
    folly::doNotOptimizeAway(to);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(Test2_RangeTestInt) {
    std::vector<int32_t> from;
    std::vector<int32_t> to;
    BENCHMARK_SUSPEND {
        from.resize(1000, 0);
        to.resize(1000);
    }
    int32_t index = 0;
    for (auto& i : from) {
        to[index++] = i;
    }
    folly::doNotOptimizeAway(to);
}
BENCHMARK_RELATIVE(Test2_TransformInt) {
    std::vector<int32_t> from;
    std::vector<int32_t> to;
    BENCHMARK_SUSPEND {
        from.resize(1000, 0);
        to.resize(1000);
    }
    std::transform(from.begin(), from.end(), to.begin(), [] (const auto& e) {
        return e;
    });
    folly::doNotOptimizeAway(to);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(Test3_RangeTestInt) {
    std::vector<int32_t> v;
    BENCHMARK_SUSPEND {
        v.resize(1000, 0);
    }
    for (auto& i : v) {
        (void)(i);
        int a{0};
        folly::doNotOptimizeAway(a);
    }
}

BENCHMARK_RELATIVE(Test3_ForEachInt) {
    std::vector<int32_t> v;
    BENCHMARK_SUSPEND {
        v.resize(1000, 0);
    }
    std::for_each(v.begin(), v.end(), [] (const auto&) {
        int a{0};
        folly::doNotOptimizeAway(a);
    });
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}


/*
Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz*

-O2
============================================================================
RangeVsTransformBenchmark.cpprelative                     time/iter  iters/s
============================================================================
Test1_RangeTestStr                                          80.60us   12.41K
Test1_TransformStr                               100.47%    80.22us   12.47K
----------------------------------------------------------------------------
Test2_RangeTestInt                                         713.67ns    1.40M
Test2_TransformInt                                99.99%   713.72ns    1.40M
----------------------------------------------------------------------------
Test3_RangeTestInt                                         383.15ns    2.61M
Test3_ForEachInt                                 100.63%   380.75ns    2.63M
============================================================================

-O3
============================================================================
RangeVsTransformBenchmark.cpprelative                     time/iter  iters/s
============================================================================
Test1_RangeTestStr                                          84.19us   11.88K
Test1_TransformStr                               100.01%    84.18us   11.88K
----------------------------------------------------------------------------
Test2_RangeTestInt                                         222.70ns    4.49M
Test2_TransformInt                                98.90%   225.18ns    4.44M
----------------------------------------------------------------------------
Test3_RangeTestInt                                         395.93ns    2.53M
Test3_ForEachInt                                 102.41%   386.61ns    2.59M
============================================================================

*/

