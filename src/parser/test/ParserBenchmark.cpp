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
auto matchConflictQuery =
    "MATCH (a)-[r:like*2..3]->(b:team) WHERE a.prop1 = b.prop2 AND (a)-(b) RETURN a, b";

size_t SimpleQuery(size_t iters, size_t nrThreads) {
  constexpr size_t ops = 500000UL;

  auto parse = [&]() {
    auto n = iters * ops;
    for (auto i = 0UL; i < n; i++) {
      auto qctx = std::make_unique<nebula::graph::QueryContext>();
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
      auto qctx = std::make_unique<nebula::graph::QueryContext>();
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

size_t MatchConflictQuery(size_t iters, size_t nrThreads) {
  constexpr size_t ops = 500000UL;

  auto parse = [&]() {
    auto n = iters * ops;
    for (auto i = 0UL; i < n; i++) {
      auto qctx = std::make_unique<nebula::graph::QueryContext>();
      // static thread_local GQLParser parser;
      GQLParser parser(qctx.get());
      auto result = parser.parse(matchConflictQuery);
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

BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM_MULTI(MatchConflictQuery, 1_thread, 1)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MatchConflictQuery, 2_thread, 2)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MatchConflictQuery, 4_thread, 4)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MatchConflictQuery, 8_thread, 8)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MatchConflictQuery, 16_thread, 16)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MatchConflictQuery, 32_thread, 32)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(MatchConflictQuery, 48_thread, 48)

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  {
    auto qctx = std::make_unique<nebula::graph::QueryContext>();
    GQLParser parser(qctx.get());
    auto result = parser.parse(simpleQuery);
    CHECK(result.ok()) << result.status();
  }
  {
    auto qctx = std::make_unique<nebula::graph::QueryContext>();
    GQLParser parser(qctx.get());
    auto result = parser.parse(complexQuery);
    CHECK(result.ok()) << result.status();
  }

  folly::runBenchmarks();
  return 0;
}

/*
Architecture:        x86_64
CPU op-mode(s):      32-bit, 64-bit
Byte Order:          Little Endian
Address sizes:       46 bits physical, 57 bits virtual
CPU(s):              128
On-line CPU(s) list: 0-127
Thread(s) per core:  2
Core(s) per socket:  32
Socket(s):           2
NUMA node(s):        2
Vendor ID:           GenuineIntel
CPU family:          6
Model:               106
Model name:          Intel(R) Xeon(R) Platinum 8352Y CPU @ 2.20GHz
============================================================================
/home/shylock.huang/nebula-tmp/src/parser/test/ParserBenchmark.cpprelative  time/iter  iters/s
============================================================================
SimpleQuery(1_thread)                                        1.70us  587.21K
SimpleQuery(2_thread)                             96.72%     1.76us  567.92K
SimpleQuery(4_thread)                             97.35%     1.75us  571.67K
SimpleQuery(8_thread)                             96.84%     1.76us  568.66K
SimpleQuery(16_thread)                            95.77%     1.78us  562.36K
SimpleQuery(32_thread)                            53.77%     3.17us  315.74K
SimpleQuery(48_thread)                            49.68%     3.43us  291.73K
----------------------------------------------------------------------------
ComplexQuery(1_thread)                                       9.94us  100.57K
ComplexQuery(2_thread)                            98.73%    10.07us   99.30K
ComplexQuery(4_thread)                           101.62%     9.78us  102.21K
ComplexQuery(8_thread)                            90.47%    10.99us   90.99K
ComplexQuery(16_thread)                           97.32%    10.22us   97.87K
ComplexQuery(32_thread)                           69.31%    14.34us   69.71K
ComplexQuery(48_thread)                           61.30%    16.22us   61.66K
----------------------------------------------------------------------------
MatchConflictQuery(1_thread)                                 3.88us  257.41K
MatchConflictQuery(2_thread)                      99.74%     3.90us  256.73K
MatchConflictQuery(4_thread)                      96.16%     4.04us  247.52K
MatchConflictQuery(8_thread)                      79.93%     4.86us  205.74K
MatchConflictQuery(16_thread)                     79.57%     4.88us  204.81K
MatchConflictQuery(32_thread)                     56.12%     6.92us  144.47K
MatchConflictQuery(48_thread)                     50.41%     7.71us  129.75K
============================================================================
*/
