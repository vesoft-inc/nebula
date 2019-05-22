/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <sys/time.h>
#include <folly/Benchmark.h>
#include <chrono>
#include "time/WallClock.h"

using nebula::time::WallClock;


BENCHMARK(gettimeofday_get_msec, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        struct timeval tp;
        gettimeofday(&tp, NULL);
        auto ts = tp.tv_sec * 1000 + tp.tv_usec / 1000;
        folly::doNotOptimizeAway(ts);
    }
}
BENCHMARK_RELATIVE(systemclock_get_time_t, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        auto tp = std::chrono::system_clock::now();
        auto ts = std::chrono::system_clock::to_time_t(tp);
        folly::doNotOptimizeAway(ts);
    }
}
BENCHMARK_RELATIVE(wallclock_get_msec_slow, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        auto ts = WallClock::slowNowInMilliSec();
        folly::doNotOptimizeAway(ts);
    }
}
BENCHMARK_RELATIVE(wallclock_get_msec_fast, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        auto ts = WallClock::fastNowInMilliSec();
        folly::doNotOptimizeAway(ts);
    }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(gettimeofday_get_sec, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        struct timeval tp;
        gettimeofday(&tp, NULL);
        auto ts = tp.tv_sec;
        folly::doNotOptimizeAway(ts);
    }
}
BENCHMARK_RELATIVE(wallclock_get_sec_slow, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        auto ts = WallClock::slowNowInSec();
        folly::doNotOptimizeAway(ts);
    }
}
BENCHMARK_RELATIVE(wallclock_get_sec_fast, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        auto ts = WallClock::fastNowInSec();
        folly::doNotOptimizeAway(ts);
    }
}


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    folly::runBenchmarks();
    return 0;
}


/*

 Tested on Intel Core i7-8650U (8 cores) with 16GB RAM

============================================================================
WallClockBenchmark.cpp                          relative  time/iter  iters/s
============================================================================
gettimeofday_get_msec                                      538.43ns    1.86M
systemclock_get_time_t                            79.38%   678.28ns    1.47M
wallclock_get_msec_slow                           79.38%   678.28ns    1.47M
wallclock_get_msec_fast                         5129.81%    10.50ns   95.27M
----------------------------------------------------------------------------
gettimeofday_get_sec                                       531.79ns    1.88M
wallclock_get_sec_slow                            78.81%   674.76ns    1.48M
wallclock_get_sec_fast                          5162.61%    10.30ns   97.08M
============================================================================

*/
