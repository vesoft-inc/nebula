/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>

#include <cstddef>
#include <cstdint>

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

size_t SnowflakeCurrencyTest(size_t iters, int threadNum) {
  constexpr size_t ops = 100000UL;

  nebula::Snowflake generator;
  std::vector<std::thread> threads;
  threads.reserve(threadNum);

  auto proc = [&iters, &generator]() {
    auto n = iters * ops;
    for (auto i = 0UL; i < n; i++) {
      int64_t id = generator.getId();
      folly::doNotOptimizeAway(id);
    }
  };

  for (int i = 0; i < threadNum; i++) {
    threads.emplace_back(std::thread(proc));
  }

  for (int i = 0; i < threadNum; i++) {
    threads[i].join();
  }

  return iters * ops * threadNum;
}

BENCHMARK_NAMED_PARAM_MULTI(SnowflakeTest, 1UL)
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 1_thread, 1)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 2_thread, 2)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 4_thread, 4)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 8_thread, 8)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 16_thread, 16)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 32_thread, 32)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 64_thread, 64)

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}

/*           Intel(R) Xeon(R) CPU E5-2673 v3 @ 2.40GHz
============================================================================
nebula/src/common/id/test/SnowflakeBenchmark.cpprelative  time/iter  iters/s
============================================================================
SnowflakeTest(1UL)                                          68.03ns   14.70M
----------------------------------------------------------------------------
SnowflakeCurrencyTest(1_thread)                            239.12ns    4.18M
SnowflakeCurrencyTest(2_thread)                   99.77%   239.66ns    4.17M
SnowflakeCurrencyTest(4_thread)                   98.65%   242.39ns    4.13M
SnowflakeCurrencyTest(8_thread)                   90.01%   265.67ns    3.76M
SnowflakeCurrencyTest(16_thread)                  68.23%   350.46ns    2.85M
SnowflakeCurrencyTest(32_thread)                  60.05%   398.22ns    2.51M
SnowflakeCurrencyTest(64_thread)                  52.51%   455.38ns    2.20M
============================================================================
*/
