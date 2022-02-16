/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>      // for addBenchmark, runBenchmarks, BENCH...
#include <folly/BenchmarkUtil.h>  // for doNotOptimizeAway
#include <folly/init/Init.h>      // for init
#include <stdint.h>               // for uint32_t

#include <chrono>  // for milliseconds, duration_cast, opera...

#include "common/time/Duration.h"  // for Duration

using nebula::time::Duration;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

BENCHMARK(steady_clock_timer, iters) {
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

int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);

  folly::runBenchmarks();
  return 0;
}

/*

 Tested on Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz x 2

============================================================================
DurationBenchmark.cpp                           relative  time/iter  iters/s
============================================================================
steady_clock_timer                                          44.45ns   22.50M
duration_timer                                   170.50%    26.07ns   38.36M
============================================================================

*/
