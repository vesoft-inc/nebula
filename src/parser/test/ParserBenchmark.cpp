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
/home/shylock.huang/nebula/src/parser/test/ParserBenchmark.cpprelative  time/iter  iters/s
============================================================================
SimpleQuery(1_thread)                                        1.30us  768.50K
SimpleQuery(2_thread)                             97.33%     1.34us  747.98K
SimpleQuery(4_thread)                             96.33%     1.35us  740.31K
SimpleQuery(8_thread)                             93.95%     1.38us  722.03K
SimpleQuery(16_thread)                            92.11%     1.41us  707.90K
SimpleQuery(32_thread)                            54.36%     2.39us  417.76K
SimpleQuery(48_thread)                            43.92%     2.96us  337.56K
----------------------------------------------------------------------------
ComplexQuery(1_thread)                                       8.07us  123.97K
ComplexQuery(2_thread)                            84.94%     9.50us  105.29K
ComplexQuery(4_thread)                            84.07%     9.60us  104.21K
ComplexQuery(8_thread)                            79.88%    10.10us   99.03K
ComplexQuery(16_thread)                           80.70%    10.00us  100.04K
ComplexQuery(32_thread)                           67.07%    12.03us   83.15K
ComplexQuery(48_thread)                           49.00%    16.46us   60.75K
----------------------------------------------------------------------------
MatchConflictQuery(1_thread)                                 3.55us  281.52K
MatchConflictQuery(2_thread)                      93.85%     3.78us  264.22K
MatchConflictQuery(4_thread)                      84.42%     4.21us  237.66K
MatchConflictQuery(8_thread)                      72.76%     4.88us  204.83K
MatchConflictQuery(16_thread)                     74.04%     4.80us  208.45K
MatchConflictQuery(32_thread)                     56.50%     6.29us  159.06K
MatchConflictQuery(48_thread)                     44.14%     8.05us  124.26K
============================================================================
*/
