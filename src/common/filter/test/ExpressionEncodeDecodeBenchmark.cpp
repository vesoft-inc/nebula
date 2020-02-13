/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <folly/Benchmark.h>
#include "filter/Expressions.h"
#include "parser/GQLParser.h"

using nebula::Expression;
using nebula::GQLParser;
using nebula::SequentialSentences;
using nebula::GoSentence;
using nebula::Status;
using nebula::StatusOr;

static Expression* getFilterExpr(SequentialSentences *sentences) {
    auto *go = dynamic_cast<GoSentence*>(sentences->sentences().front());
    return go->whereClause()->filter();
}

size_t Encode(size_t iters, std::string query) {
    constexpr size_t ops = 1000000UL;

    query = "GO FROM 1 AS p OVER q WHERE " + query;
    Expression *expr;
    StatusOr<std::unique_ptr<SequentialSentences>> result;
    BENCHMARK_SUSPEND {
        GQLParser parser;
        result = parser.parse(query);
        if (!result.ok()) {
             return 0;
        }
        expr = getFilterExpr(result.value().get());
    }

    auto i = 0UL;
    while (i++ < ops * iters) {
        auto buffer = Expression::encode(expr);
        folly::doNotOptimizeAway(i);
        folly::doNotOptimizeAway(buffer);
    }

    return iters * ops;
}

size_t Decode(size_t iters, std::string query) {
    constexpr size_t ops = 1000000UL;

    query = "GO FROM 1 AS p OVER q WHERE " + query;
    std::string buffer;
    BENCHMARK_SUSPEND {
        GQLParser parser;
        auto result = parser.parse(query);
        if (!result.ok()) {
             return 0;
        }
        auto *expr = getFilterExpr(result.value().get());
        buffer = Expression::encode(expr);
    }

    auto i = 0UL;
    while (i++ < ops * iters) {
        auto result = Expression::decode(buffer);
        folly::doNotOptimizeAway(i);
        folly::doNotOptimizeAway(result);
    }

    return iters * ops;
}

auto simpleQuery =  "123 + 123 - 123 * 123 / 123";
auto complexQuery =  "alias.prop1 + alias.prop2 * alias.prop3 > alias.prop4 && "
                     "alias.prop5 == alias.prop6";

BENCHMARK_NAMED_PARAM_MULTI(Encode, Simple, simpleQuery)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(Encode, Complex, complexQuery)

BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM_MULTI(Decode, Simple, simpleQuery)
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(Decode, Complex, complexQuery)

int
main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/*           Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz
============================================================================
ExpressionEncodeDecodeBenchmark.cpp             relative  time/iter  iters/s
============================================================================
Encode(Simple)                                             262.15ns    3.81M
Encode(Complex)                                   74.91%   349.94ns    2.86M
----------------------------------------------------------------------------
Decode(Simple)                                             284.99ns    3.51M
Decode(Complex)                                   42.00%   678.53ns    1.47M
============================================================================
 */
