/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <folly/Benchmark.h>

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "parser/GQLParser.h"

using nebula::GQLParser;

auto simpleQuery = "USE myspace";
auto complexQuery =
    "GO 2 STEPS FROM 123456789 OVER myedge "
    "WHERE alias.prop1 + alias.prop2 * alias.prop3 > alias.prop4 AND "
    "alias.prop5 == alias.prop6 YIELD 1+1+1+1+1+1+1+1 AS first, 2 AS second";

auto qctx = std::make_unique<nebula::graph::QueryContext>();

size_t SimpleQuery(size_t iters, size_t nrThreads) {
  constexpr size_t ops = 500000UL;

  auto parse = [&]() {
    auto n = iters * ops;
    for (auto i = 0UL; i < n; i++) {
      // static thread_local GQLParser parser;
      GQLParser parser(qctx.get());
      auto result = parser.parse(simpleQuery);
      folly::doNotOptimizeAway(result);
    }
  };

  std::vector<std::thread> workers;
  for (auto i = 0u; i < nrThreads; i++) {
    workers.emplace_back(parse);
  }
  for (auto i = 0u; i < nrThreads; i++) {
    workers[i].join();
  }

  return iters * ops;
}

size_t ComplexQuery(size_t iters, size_t nrThreads) {
  constexpr size_t ops = 500000UL;

  auto parse = [&]() {
    auto n = iters * ops;
    for (auto i = 0UL; i < n; i++) {
      // static thread_local GQLParser parser;
      GQLParser parser(qctx.get());
      auto result = parser.parse(complexQuery);
      folly::doNotOptimizeAway(result);
    }
  };

  std::vector<std::thread> workers;
  for (auto i = 0u; i < nrThreads; i++) {
    workers.emplace_back(parse);
  }
  for (auto i = 0u; i < nrThreads; i++) {
    workers[i].join();
  }

  return iters * ops;
}

BENCHMARK_NAMED_PARAM_MULTI(SimpleQuery, 1_thread, 1)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 2_thread, 2)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 4_thread, 4)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 8_thread, 8)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 16_thread, 16)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 32_thread, 32)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 48_thread, 48)

BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM_MULTI(ComplexQuery, 1_thread, 1)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 2_thread, 2)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 4_thread, 4)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 8_thread, 8)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 16_thread, 16)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 32_thread, 32)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 48_thread, 48)

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  {
    GQLParser parser(qctx.get());
    auto result = parser.parse(simpleQuery);
    CHECK(result.ok()) << result.status();
  }
  {
    GQLParser parser(qctx.get());
    auto result = parser.parse(complexQuery);
    CHECK(result.ok()) << result.status();
  }

  folly::runBenchmarks();
  return 0;
}

/*           Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz, 20core 40thread

                                Non-Thread-Local
============================================================================
src/parser/test/ParserBenchmark.cpp             relative  time/iter  iters/s
============================================================================
SimpleQuery(1_thread)                                      892.36ns    1.12M
SimpleQuery(2_thread)                             67.21%     1.33us  753.21K
SimpleQuery(4_thread)                             41.68%     2.14us  467.11K
SimpleQuery(8_thread)                             19.62%     4.55us  219.92K
SimpleQuery(16_thread)                             9.62%     9.28us  107.76K
SimpleQuery(32_thread)                             5.32%    16.76us   59.66K
SimpleQuery(48_thread)                             3.98%    22.43us   44.58K
----------------------------------------------------------------------------
ComplexQuery(1_thread)                                       6.40us  156.16K
ComplexQuery(2_thread)                           100.48%     6.37us  156.91K
ComplexQuery(4_thread)                            92.96%     6.89us  145.16K
ComplexQuery(8_thread)                            82.83%     7.73us  129.36K
ComplexQuery(16_thread)                           61.58%    10.40us   96.16K
ComplexQuery(32_thread)                           35.98%    17.80us   56.19K
ComplexQuery(48_thread)                           26.24%    24.41us   40.97K
============================================================================
                                Thread-Local(Reentrant)
============================================================================
SimpleQuery(1_thread)                                      496.38ns    2.01M
SimpleQuery(2_thread)                             91.86%   540.35ns    1.85M
SimpleQuery(4_thread)                             89.51%   554.53ns    1.80M
SimpleQuery(8_thread)                             89.86%   552.42ns    1.81M
SimpleQuery(16_thread)                            58.34%   850.83ns    1.18M
SimpleQuery(32_thread)                            56.01%   886.22ns    1.13M
SimpleQuery(48_thread)                            39.20%     1.27us  789.64K
----------------------------------------------------------------------------
ComplexQuery(1_thread)                                       5.15us  194.04K
ComplexQuery(2_thread)                            93.48%     5.51us  181.38K
ComplexQuery(4_thread)                            94.62%     5.45us  183.59K
ComplexQuery(8_thread)                            92.52%     5.57us  179.53K
ComplexQuery(16_thread)                           79.93%     6.45us  155.09K
ComplexQuery(32_thread)                           60.09%     8.58us  116.60K
ComplexQuery(48_thread)                           44.15%    11.67us   85.67K
============================================================================
*/

/*           Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz, 64core 128thread
                  Short Expression without depth-check
============================================================================
ParserBenchmark.cpp                              relative  time/iter  iters/s
============================================================================
SimpleQuery(1_thread)                                       14.27us   70.08K
SimpleQuery(2_thread)                             99.87%    14.29us   69.99K
SimpleQuery(4_thread)                             99.97%    14.27us   70.06K
SimpleQuery(8_thread)                             99.04%    14.41us   69.40K
SimpleQuery(16_thread)                            99.79%    14.30us   69.93K
SimpleQuery(32_thread)                            93.93%    15.19us   65.82K
SimpleQuery(48_thread)                            83.38%    17.12us   58.43K
----------------------------------------------------------------------------
ComplexQuery(1_thread)                                      58.73us   17.03K
ComplexQuery(2_thread)                            91.71%    64.04us   15.62K
ComplexQuery(4_thread)                            76.31%    76.96us   12.99K
ComplexQuery(8_thread)                            34.66%   169.48us    5.90K
ComplexQuery(16_thread)                           13.28%   442.11us    2.26K
ComplexQuery(32_thread)                            4.06%     1.45ms   690.60
ComplexQuery(48_thread)                            2.64%     2.22ms   450.27
============================================================================

                    Long Expression without depth-check
============================================================================
ParserBenchmark.cpp                             relative  time/iter  iters/s
============================================================================
SimpleQuery(1_thread)                                       14.26us   70.15K
SimpleQuery(2_thread)                             99.78%    14.29us   70.00K
SimpleQuery(4_thread)                             99.36%    14.35us   69.70K
SimpleQuery(8_thread)                             99.39%    14.34us   69.73K
SimpleQuery(16_thread)                            97.04%    14.69us   68.07K
SimpleQuery(32_thread)                            86.41%    16.50us   60.61K
SimpleQuery(48_thread)                            76.56%    18.62us   53.71K
----------------------------------------------------------------------------
ComplexQuery(1_thread)                                      70.21us   14.24K
ComplexQuery(2_thread)                            91.32%    76.89us   13.01K
ComplexQuery(4_thread)                            73.01%    96.17us   10.40K
ComplexQuery(8_thread)                            28.98%   242.26us    4.13K
ComplexQuery(16_thread)                           10.12%   693.63us    1.44K

                      Long Expression with depth-check
============================================================================
ParserBenchmark.cpp                             relative  time/iter  iters/s
============================================================================
SimpleQuery(1_thread)                                       14.28us   70.04K
SimpleQuery(2_thread)                            100.08%    14.27us   70.10K
SimpleQuery(4_thread)                            100.04%    14.27us   70.07K
SimpleQuery(8_thread)                             99.66%    14.33us   69.80K
SimpleQuery(16_thread)                            99.36%    14.37us   69.59K
SimpleQuery(32_thread)                            91.71%    15.57us   64.23K
SimpleQuery(48_thread)                            84.16%    16.96us   58.95K
----------------------------------------------------------------------------
ComplexQuery(1_thread)                                      73.13us   13.67K
ComplexQuery(2_thread)                            86.52%    84.53us   11.83K
ComplexQuery(4_thread)                            77.40%    94.48us   10.58K
ComplexQuery(8_thread)                            25.52%   286.61us    3.49K
ComplexQuery(16_thread)                           10.70%   683.78us    1.46K
ComplexQuery(32_thread)                            3.50%     2.09ms   478.55
Killed
*/
