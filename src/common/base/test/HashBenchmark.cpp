/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/base/Base.h"
#include <folly/Benchmark.h>
#include "common/base/MurmurHash2.h"

using nebula::MurmurHash2;

std::string makeString(size_t size) {
    std::string str;
    str.resize(size);
    for (auto &c : str) {
        c = folly::Random::rand32() % (0x7E/*~*/ - 0x21/*!*/) + 0x21;
    }
    return str;
}

size_t StdHashTest(size_t iters, size_t size) {
    constexpr size_t ops = 1000000UL;

    std::hash<std::string> hash;
    auto str = makeString(size);
    auto i = 0UL;
    while (i++ < ops * iters) {
        auto hv = hash(str);
        folly::doNotOptimizeAway(hv);
    }

    return iters * ops;
}

size_t MurmurHash2Test(size_t iters, size_t size) {
    constexpr size_t ops = 1000000UL;

    MurmurHash2 hash;
    auto str = makeString(size);
    auto i = 0UL;
    while (i++ < ops * iters) {
        auto hv = hash(str);
        folly::doNotOptimizeAway(hv);
    }

    return iters * ops;
}

BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 1Byte, 1UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 1Byte, 1UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 2Byte, 2UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 2Byte, 2UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 3Byte, 3UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 3Byte, 3UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 4Byte, 4UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 4Byte, 4UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 5Byte, 5UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 5Byte, 5UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 6Byte, 6UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 6Byte, 6UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 7Byte, 7UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 7Byte, 7UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 8Byte, 8UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 8Byte, 8UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 9Byte, 9UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 9Byte, 9UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 10Byte, 10UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 10Byte, 10UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 64Byte, 64UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 64Byte, 64UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 256Byte, 256UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 256Byte, 256UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 1024Byte, 1024UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 1024Byte, 1024UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(StdHashTest, 4096Byte, 4096UL)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MurmurHash2Test, 4096Byte, 4096UL)

int
main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/*           Intel(R) Xeon(R) CPU E5-2673 v3 @ 2.40GHz
============================================================================
src/common/base/test/HashBenchmark.cpp          relative  time/iter  iters/s
============================================================================
StdHashTest(1Byte)                                           4.53ns  220.75M
MurmurHash2Test(1Byte)                           293.53%     1.54ns  647.95M
----------------------------------------------------------------------------
StdHashTest(2Byte)                                           5.05ns  198.20M
MurmurHash2Test(2Byte)                           271.92%     1.86ns  538.93M
----------------------------------------------------------------------------
StdHashTest(3Byte)                                           5.86ns  170.75M
MurmurHash2Test(3Byte)                           278.04%     2.11ns  474.76M
----------------------------------------------------------------------------
StdHashTest(4Byte)                                           6.65ns  150.37M
MurmurHash2Test(4Byte)                           291.51%     2.28ns  438.34M
----------------------------------------------------------------------------
StdHashTest(5Byte)                                           7.21ns  138.77M
MurmurHash2Test(5Byte)                           270.39%     2.67ns  375.23M
----------------------------------------------------------------------------
StdHashTest(6Byte)                                           7.91ns  126.38M
MurmurHash2Test(6Byte)                           264.52%     2.99ns  334.30M
----------------------------------------------------------------------------
StdHashTest(7Byte)                                           8.75ns  114.26M
MurmurHash2Test(7Byte)                           258.92%     3.38ns  295.85M
----------------------------------------------------------------------------
StdHashTest(8Byte)                                           4.61ns  216.97M
MurmurHash2Test(8Byte)                           173.35%     2.66ns  376.11M
----------------------------------------------------------------------------
StdHashTest(9Byte)                                           5.56ns  179.73M
MurmurHash2Test(9Byte)                           187.64%     2.97ns  337.25M
----------------------------------------------------------------------------
StdHashTest(10Byte)                                          6.13ns  163.24M
MurmurHash2Test(10Byte)                          196.97%     3.11ns  321.53M
----------------------------------------------------------------------------
StdHashTest(64Byte)                                         12.76ns   78.40M
MurmurHash2Test(64Byte)                          117.30%    10.87ns   91.96M
----------------------------------------------------------------------------
StdHashTest(256Byte)                                        48.69ns   20.54M
MurmurHash2Test(256Byte)                         108.87%    44.72ns   22.36M
----------------------------------------------------------------------------
StdHashTest(1024Byte)                                      204.19ns    4.90M
MurmurHash2Test(1024Byte)                         98.92%   206.43ns    4.84M
----------------------------------------------------------------------------
StdHashTest(4096Byte)                                      825.29ns    1.21M
MurmurHash2Test(4096Byte)                         98.29%   839.61ns    1.19M
============================================================================
 */
