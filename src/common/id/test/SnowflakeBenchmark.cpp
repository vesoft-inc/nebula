/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>

#include <cstddef>

#include "common/base/Base.h"
#include "common/id/Snowflake.h"

size_t SnowflakeTest(size_t iters) {
  constexpr size_t ops = 10UL;

  nebula::Snowflake generator;
  auto i = 0UL;
  while (i++ < ops * iters) {
    int64_t id = generator.getId();
    folly::doNotOptimizeAway(id);
  }

  return iters * ops;
}

void proc(nebula::Snowflake& generator, size_t times) {
  for (size_t i = 0; i < times; i++) {
    int64_t id = generator.getId();
    folly::doNotOptimizeAway(id);
  }
}

size_t SnowflakeCurrencyTest(size_t iters) {
  int threadNum = 16;
  constexpr size_t ops = 100000UL;
  size_t times = ops * iters;

  nebula::Snowflake generator;
  std::vector<std::thread> threads;
  threads.reserve(threadNum);

  for (int i = 0; i < threadNum; i++) {
    threads.emplace_back(std::thread(proc, std::ref(generator), std::ref(times)));
  }

  for (int i = 0; i < threadNum; i++) {
    threads[i].join();
  }

  return times * threadNum;
}

BENCHMARK_NAMED_PARAM_MULTI(SnowflakeTest, 1UL)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 1UL)

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
/*           Intel(R) Xeon(R) CPU E5-2673 v3 @ 2.40GHz
============================================================================
nebula/src/common/id/test/SnowflakeBenchmark.cpprelative  time/iter  iters/s
============================================================================
SnowflakeTest(1UL)                                          75.61ns   13.23M
----------------------------------------------------------------------------
SnowflakeCurrencyTest(1UL)                                   5.09us  196.37K
----------------------------------------------------------------------------
============================================================================
*/
