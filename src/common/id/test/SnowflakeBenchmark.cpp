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
// size_t ComplexQuery(size_t iters, size_t nrThreads) {
//   constexpr size_t ops = 500000UL;

//   auto parse = [&]() {
//     auto n = iters * ops;
//     for (auto i = 0UL; i < n; i++) {
//       // static thread_local GQLParser parser;
//       GQLParser parser(qctx.get());
//       auto result = parser.parse(complexQuery);
//       folly::doNotOptimizeAway(result);
//     }
//   };

//   std::vector<std::thread> workers;
//   for (auto i = 0u; i < nrThreads; i++) {
//     workers.emplace_back(parse);
//   }
//   for (auto i = 0u; i < nrThreads; i++) {
//     workers[i].join();
//   }

//   return iters * ops;
// }

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

  return iters * ops;
}

BENCHMARK_NAMED_PARAM_MULTI(SnowflakeTest, 1UL)
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 1_thread, 1)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 2_thread, 2)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 4_thread, 4)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 8_thread, 8)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 16_thread, 16)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SnowflakeCurrencyTest, 32_thread, 32)

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
/*           Intel(R) Xeon(R) CPU E5-2673 v3 @ 2.40GHz
============================================================================
nebula/src/common/id/test/SnowflakeBenchmark.cpprelative  time/iter  iters/s
============================================================================
SnowflakeTest(1UL)                                          68.79ns   14.54M
----------------------------------------------------------------------------
SnowflakeCurrencyTest(1_thread)                            238.94ns    4.19M
SnowflakeCurrencyTest(2_thread)                   49.84%   479.43ns    2.09M
SnowflakeCurrencyTest(4_thread)                   24.64%   969.60ns    1.03M
SnowflakeCurrencyTest(8_thread)                   12.18%     1.96us  509.73K
SnowflakeCurrencyTest(16_thread)                   4.94%     4.83us  206.92K
SnowflakeCurrencyTest(32_thread)                   2.09%    11.44us   87.42K
============================================================================
*/
