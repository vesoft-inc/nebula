/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <sys/time.h>
#include <folly/Benchmark.h>
#include <chrono>
#include "common/time/WallClock.h"

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

 Tested on Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz x 2

============================================================================
WallClockBenchmark.cpp                          relative  time/iter  iters/s
============================================================================
gettimeofday_get_msec                                       24.40ns   40.99M
systemclock_get_time_t                           129.21%    18.88ns   52.96M
wallclock_get_msec_slow                          129.91%    18.78ns   53.25M
wallclock_get_msec_fast                          172.47%    14.15ns   70.69M
----------------------------------------------------------------------------
gettimeofday_get_sec                                        17.21ns   58.10M
wallclock_get_sec_slow                            95.37%    18.05ns   55.41M
wallclock_get_sec_fast                           129.28%    13.31ns   75.12M
============================================================================

*/
