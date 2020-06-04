/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>
#include "common/expression/test/ExpressionContextMock.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/RelationalExpression.h"

nebula::ExpressionContextMock gExpCtxt;

namespace nebula {
size_t add2Constant(size_t iters) {
    constexpr size_t ops = 1000000UL;
    Value eval;
    for (size_t i = 0; i < iters * ops; ++i) {
        ArithmeticExpression add(
                Expression::Kind::kAdd, new ConstantExpression(1), new ConstantExpression(2));
        eval = Expression::eval(&add, gExpCtxt);
    }
    folly::doNotOptimizeAway(eval);
    return iters * ops;
}

size_t add3Constant(size_t iters) {
    constexpr size_t ops = 1000000UL;
    Value eval;
    for (size_t i = 0; i < iters * ops; ++i) {
        ArithmeticExpression add(
                Expression::Kind::kAdd,
                new ArithmeticExpression(
                    Expression::Kind::kAdd,
                    new ConstantExpression(1), new ConstantExpression(2)),
                new ConstantExpression(3));
        eval = Expression::eval(&add, gExpCtxt);
    }
    folly::doNotOptimizeAway(eval);
    return iters * ops;
}

size_t add2Constant1EdgeProp(size_t iters) {
    constexpr size_t ops = 1000000UL;
    Value eval;
    for (size_t i = 0; i < iters * ops; ++i) {
        ArithmeticExpression add(
                Expression::Kind::kAdd,
                new ArithmeticExpression(
                    Expression::Kind::kAdd,
                    new ConstantExpression(1), new ConstantExpression(2)),
                new EdgePropertyExpression(new std::string("e1"), new std::string("int")));
        eval = Expression::eval(&add, gExpCtxt);
    }
    folly::doNotOptimizeAway(eval);
    return iters * ops;
}

size_t concat2String(size_t iters) {
    constexpr size_t ops = 1000000UL;
    Value eval;
    for (size_t i = 0; i < iters * ops; ++i) {
        ArithmeticExpression add(
                Expression::Kind::kAdd,
                new EdgePropertyExpression(new std::string("e1"), new std::string("string16")),
                new EdgePropertyExpression(new std::string("e1"), new std::string("string16")));
        eval = Expression::eval(&add, gExpCtxt);
    }
    folly::doNotOptimizeAway(eval);
    return iters * ops;
}

size_t inList(size_t iters) {
    constexpr size_t ops = 1000000UL;
    Value eval;
    for (size_t i = 0; i < iters * ops; ++i) {
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression("aaaa"),
                new EdgePropertyExpression(new std::string("e1"), new std::string("list")));
        eval = Expression::eval(&expr, gExpCtxt);
    }
    folly::doNotOptimizeAway(eval);
    return iters * ops;
}

size_t isNull(size_t iters, const char* var) {
    constexpr size_t ops = 1000000UL;
    Value eval;
    for (size_t i = 0; i < iters * ops; ++i) {
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new EdgePropertyExpression(new std::string("e1"), new std::string(var)),
                new ConstantExpression(Value(NullType::NaN)));
        eval = Expression::eval(&expr, gExpCtxt);
    }
    folly::doNotOptimizeAway(eval);
    return iters * ops;
}

size_t isListEq(size_t iters, const char* var) {
    constexpr size_t ops = 1000000UL;
    Value eval;
    for (size_t i = 0; i < iters * ops; ++i) {
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new EdgePropertyExpression(new std::string("e1"), new std::string(var)),
                new EdgePropertyExpression(new std::string("e1"), new std::string(var)));
        eval = Expression::eval(&expr, gExpCtxt);
    }
    folly::doNotOptimizeAway(eval);
    return iters * ops;
}

// TODO(cpw): more test cases.

BENCHMARK_NAMED_PARAM_MULTI(add2Constant, 1_add_2)
BENCHMARK_NAMED_PARAM_MULTI(add3Constant, 1_add_2_add_3)
BENCHMARK_NAMED_PARAM_MULTI(add2Constant1EdgeProp, 1_add_2_add_e1_int)
BENCHMARK_NAMED_PARAM_MULTI(concat2String, concat_string_string)
BENCHMARK_NAMED_PARAM_MULTI(inList, in_list)
BENCHMARK_NAMED_PARAM_MULTI(isNull, is_list_eq_null, "list")
BENCHMARK_NAMED_PARAM_MULTI(isNull, is_listoflist_eq_Null, "list_of_list")
BENCHMARK_NAMED_PARAM_MULTI(isListEq, is_list_eq_list, "list")
BENCHMARK_NAMED_PARAM_MULTI(isListEq, is_listoflist_eq_listoflist, "list_of_list")
}  // namespace nebula

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/*
Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz
============================================================================
ExpressionBenchmark.cpprelative                           time/iter  iters/s
============================================================================
add2Constant(1_add_2)                                      101.99ns    9.80M
add3Constant(1_add_2_add_3)                                186.64ns    5.36M
add2Constant1EdgeProp(1_add_2_add_e1_int)                  307.04ns    3.26M
concat2String(concat_string_string)                        356.84ns    2.80M
inList(in_list)                                            215.56ns    4.64M
isNull(is_list_eq_null)                                    189.74ns    5.27M
isNull(is_listoflist_eq_Null)                              191.31ns    5.23M
isListEq(is_list_eq_list)                                  474.51ns    2.11M
isListEq(is_listoflist_eq_listoflist)                        3.48us  287.02K
============================================================================
*/
