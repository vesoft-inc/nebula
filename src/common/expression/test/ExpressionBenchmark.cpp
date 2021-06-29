/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>
#include "common/expression/test/TestBase.h"

namespace nebula {
size_t add2Constant(size_t iters) {
    constexpr size_t ops = 1000000UL;
    auto expr = ArithmeticExpression::makeAdd(
        &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, 2));
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t add3Constant(size_t iters) {
    constexpr size_t ops = 1000000UL;
    auto expr = ArithmeticExpression::makeAdd(
        &pool,
        ArithmeticExpression::makeAdd(
            &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, 2)),
        ConstantExpression::make(&pool, 3));
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t add2Constant1EdgeProp(size_t iters) {
    constexpr size_t ops = 1000000UL;
    auto expr = ArithmeticExpression::makeAdd(
        &pool,
        ArithmeticExpression::makeAdd(
            &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, 2)),
        EdgePropertyExpression::make(&pool, "e1", "int"));
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t concat2String(size_t iters) {
    constexpr size_t ops = 1000000UL;
    auto expr =
        ArithmeticExpression::makeAdd(&pool,
                                      EdgePropertyExpression::make(&pool, "e1", "string16"),
                                      EdgePropertyExpression::make(&pool, "e1", "string16"));
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t inList(size_t iters) {
    constexpr size_t ops = 1000000UL;
    auto expr =
        RelationalExpression::makeIn(&pool,
                                     ConstantExpression::make(&pool, "aaaa"),
                                     VariablePropertyExpression::make(&pool, "var", "list"));
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t isNull(size_t iters, const char* prop) {
    constexpr size_t ops = 1000000UL;
    auto expr = RelationalExpression::makeEQ(&pool,
                                             VariablePropertyExpression::make(&pool, "var", prop),
                                             ConstantExpression::make(&pool, Value(NullType::NaN)));
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t isListEq(size_t iters, const char* prop) {
    constexpr size_t ops = 1000000UL;
    auto expr = RelationalExpression::makeEQ(&pool,
                                             VariablePropertyExpression::make(&pool, "var", prop),
                                             VariablePropertyExpression::make(&pool, "var", prop));
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t getSrcProp(size_t iters, const char* prop) {
    constexpr size_t ops = 1000000UL;
    // $^.source.int
    auto expr = SourcePropertyExpression::make(&pool, "source", prop);
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t getDstProp(size_t iters, const char* prop) {
    constexpr size_t ops = 1000000UL;
    // $^.dest.int
    auto expr = DestPropertyExpression::make(&pool, "dest", prop);
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters * ops;
}

size_t getEdgeProp(size_t iters, const char* prop) {
    constexpr size_t ops = 1000000UL;
    // e1.int
    auto expr = EdgePropertyExpression::make(&pool, "e1", prop);
    for (size_t i = 0; i < iters * ops; ++i) {
        Value eval = Expression::eval(expr, gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
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
BENCHMARK_NAMED_PARAM_MULTI(getSrcProp, ger_src_prop_int, "int")
BENCHMARK_NAMED_PARAM_MULTI(getSrcProp, ger_src_prop_string, "string16")
BENCHMARK_NAMED_PARAM_MULTI(getDstProp, ger_dst_prop_int, "int")
BENCHMARK_NAMED_PARAM_MULTI(getDstProp, ger_dst_prop_string, "string16")
BENCHMARK_NAMED_PARAM_MULTI(getEdgeProp, ger_edge_prop_int, "int")
BENCHMARK_NAMED_PARAM_MULTI(getEdgeProp, ger_edge_prop_string, "string16")
}   // namespace nebula

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/*
Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz

The first beginning implementation of expressions which return Value.
============================================================================
ExpressionBenchmark.cpprelative                           time/iter  iters/s
============================================================================
add2Constant(1_add_2)                                       30.56ns   32.73M
add3Constant(1_add_2_add_3)                                 73.70ns   13.57M
add2Constant1EdgeProp(1_add_2_add_e1_int)                   85.09ns   11.75M
concat2String(concat_string_string)                        182.76ns    5.47M
inList(in_list)                                            369.83ns    2.70M
isNull(is_list_eq_null)                                    342.08ns    2.92M
isNull(is_listoflist_eq_Null)                                4.88us  204.92K
isListEq(is_list_eq_list)                                  858.80ns    1.16M
isListEq(is_listoflist_eq_listoflist)                       12.90us   77.55K
getSrcProp(ger_src_prop_int)                                43.96ns   22.75M
getSrcProp(ger_src_prop_string)                             53.88ns   18.56M
getDstProp(ger_dst_prop_int)                                43.90ns   22.78M
getDstProp(ger_dst_prop_string)                             53.92ns   18.54M
getEdgeProp(ger_edge_prop_int)                              44.00ns   22.73M
getEdgeProp(ger_edge_prop_string)                           54.07ns   18.49M
============================================================================

An implementation of expressions which return const Values&
============================================================================
ExpressionBenchmark.cpprelative                           time/iter  iters/s
============================================================================
add2Constant(1_add_2)                                       33.33ns   30.00M
add3Constant(1_add_2_add_3)                                 59.36ns   16.85M
add2Constant1EdgeProp(1_add_2_add_e1_int)                  111.15ns    9.00M
concat2String(concat_string_string)                        261.77ns    3.82M
inList(in_list)                                             65.94ns   15.17M
isNull(is_list_eq_null)                                     52.20ns   19.16M
isNull(is_listoflist_eq_Null)                               52.62ns   19.00M
isListEq(is_list_eq_list)                                  264.17ns    3.79M
isListEq(is_listoflist_eq_listoflist)                        3.20us  312.83K
getSrcProp(ger_src_prop_int)                                52.02ns   19.22M
getSrcProp(ger_src_prop_string)                             87.57ns   11.42M
getDstProp(ger_dst_prop_int)                                51.55ns   19.40M
getDstProp(ger_dst_prop_string)                             87.82ns   11.39M
getEdgeProp(ger_edge_prop_int)                              51.88ns   19.27M
getEdgeProp(ger_edge_prop_string)                           88.41ns   11.31M
============================================================================

The latest(2020/07/13) vesion of the implementation of expressions,
which return Value for getting edge props and src props in ExpressionContext.
============================================================================
ExpressionBenchmark.cpprelative                           time/iter  iters/s
============================================================================
add2Constant(1_add_2)                                       33.36ns   29.98M
add3Constant(1_add_2_add_3)                                 59.73ns   16.74M
add2Constant1EdgeProp(1_add_2_add_e1_int)                  109.50ns    9.13M
concat2String(concat_string_string)                        264.91ns    3.77M
inList(in_list)                                             67.54ns   14.81M
isNull(is_list_eq_null)                                     51.71ns   19.34M
isNull(is_listoflist_eq_Null)                               52.95ns   18.89M
isListEq(is_list_eq_list)                                  266.94ns    3.75M
isListEq(is_listoflist_eq_listoflist)                        3.20us  312.32K
getSrcProp(ger_src_prop_int)                                61.45ns   16.27M
getSrcProp(ger_src_prop_string)                            104.71ns    9.55M
getDstProp(ger_dst_prop_int)                                43.45ns   23.01M
getDstProp(ger_dst_prop_string)                             64.98ns   15.39M
getEdgeProp(ger_edge_prop_int)                              61.43ns   16.28M
getEdgeProp(ger_edge_prop_string)                          103.98ns    9.62M
============================================================================
*/
