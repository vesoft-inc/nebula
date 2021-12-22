/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>

#include "common/base/Base.h"
#include "common/id/Snowflake.h"

size_t SnowflakeTest(size_t iters) {
  constexpr size_t ops = 1000000UL;

  nebula::Snowflake generator;
  generator.mockInitWorkerId(0);
  auto i = 0UL;
  while (i++ < ops * iters) {
    int64_t id = generator.getId();
    folly::doNotOptimizeAway(id);
  }

  return iters * ops;
}

BENCHMARK_NAMED_PARAM_MULTI(SnowflakeTest, 1UL)
BENCHMARK_DRAW_LINE();

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}

/*           Intel(R) Xeon(R) CPU E5-2673 v3 @ 2.40GHz
============================================================================
/home/jakevin.wen/code/nebula/src/common/id/test/SnowflakeBenchmark.cpprelative  time/iter  iters/s
============================================================================
SnowflakeTest(1UL)                                         243.21ns    4.11M
----------------------------------------------------------------------------
============================================================================
*/
