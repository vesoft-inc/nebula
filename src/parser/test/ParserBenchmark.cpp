/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <folly/Benchmark.h>
#include "parser/GQLParser.h"
#include "common/filter/Expressions.h"

using nebula::GQLParser;

auto simpleQuery =  "USE myspace";
auto complexQuery =  "GO UPTO 2 STEPS FROM 123456789 OVER myedge "
                     "WHERE alias.prop1 + alias.prop2 * alias.prop3 > alias.prop4 && "
                     "alias.prop5 == alias.prop6 YIELD 1 AS first, 2 AS second";


size_t SimpleQuery(size_t iters, size_t nrThreads) {
    constexpr size_t ops = 500000UL;

    auto parse = [&] () {
        auto n = iters * ops;
        for (auto i = 0UL; i < n; i++) {
            GQLParser parser;
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

    auto parse = [&] () {
        auto n = iters * ops;
        for (auto i = 0UL; i < n; i++) {
            GQLParser parser;
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

BENCHMARK_NAMED_PARAM_MULTI(SimpleQuery, 1_thread, 1);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 2_thread, 2);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 4_thread, 4);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 8_thread, 8);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 16_thread, 16);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 32_thread, 32);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(SimpleQuery, 48_thread, 48);

BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM_MULTI(ComplexQuery, 1_thread, 1);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 2_thread, 2);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 4_thread, 4);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 8_thread, 8);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 16_thread, 16);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 32_thread, 32);
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(ComplexQuery, 48_thread, 48);

int
main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    {
        GQLParser parser;
        auto result = parser.parse(simpleQuery);
        CHECK(result.ok()) << result.status();
    }
    {
        GQLParser parser;
        auto result = parser.parse(complexQuery);
        CHECK(result.ok()) << result.status();
    }

    folly::runBenchmarks();
    return 0;
}

/*           Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz, 20core 40thread
============================================================================
src/parser/test/ParserBenchmark.cpp             relative  time/iter  iters/s
============================================================================
SimpleQuery(1_thread)                                        1.16us  860.26K
SimpleQuery(2_thread)                             56.46%     2.06us  485.71K
SimpleQuery(4_thread)                             24.76%     4.69us  213.04K
SimpleQuery(8_thread)                             12.67%     9.17us  109.04K
SimpleQuery(16_thread)                             5.86%    19.85us   50.39K
SimpleQuery(32_thread)                             3.38%    34.36us   29.10K
SimpleQuery(48_thread)                             2.57%    45.24us   22.11K
----------------------------------------------------------------------------
ComplexQuery(1_thread)                                       6.02us  166.08K
ComplexQuery(2_thread)                            92.93%     6.48us  154.33K
ComplexQuery(4_thread)                            80.81%     7.45us  134.20K
ComplexQuery(8_thread)                            57.20%    10.53us   94.99K
ComplexQuery(16_thread)                           29.74%    20.25us   49.39K
ComplexQuery(32_thread)                           16.76%    35.93us   27.84K
ComplexQuery(48_thread)                           12.45%    48.38us   20.67K
============================================================================
 */
