/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "time/Duration.h"

using nebula::time::Duration;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

decltype(steady_clock::now()) scThen;
decltype(steady_clock::now()) scNow;

Duration dur(false);


void prepareDuration() {
    // steady_clock
    scThen = steady_clock::now();
    usleep(123456);
    scNow = steady_clock::now();

    // Duration
    dur.reset();
    usleep(123456);
    dur.pause();
}


BENCHMARK_DRAW_LINE();

BENCHMARK(steady_clock_ticks_to_msec, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        auto diff = (duration_cast<milliseconds>(scNow - scThen)).count();
        folly::doNotOptimizeAway(diff);
    }
}
BENCHMARK_RELATIVE(duration_ticks_to_msec, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        auto diff = dur.elapsedInMSec();
        folly::doNotOptimizeAway(diff);
    }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(stead_clock_timer, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        auto start = steady_clock::now();
        auto end = steady_clock::now();
        auto diffInMSec = (duration_cast<milliseconds>(end - start)).count();
        folly::doNotOptimizeAway(diffInMSec);
    }
}
BENCHMARK_RELATIVE(duration_timer, iters) {
    for (uint32_t i = 0; i < iters; i++) {
        Duration d;
        auto diffInMSec = d.elapsedInMSec();
        folly::doNotOptimizeAway(diffInMSec);
    }
}

BENCHMARK_DRAW_LINE();


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    prepareDuration();

    folly::runBenchmarks();
    return 0;
}


/*
 Tested on Intel Core i5-4300U (4 cores) with 8GB RAM
*/
