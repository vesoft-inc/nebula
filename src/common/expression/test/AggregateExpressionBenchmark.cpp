/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>
#include <memory>
#include "common/base/ObjectPool.h"
#include "common/expression/AggregateExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/test/ExpressionContextMock.h"

nebula::ExpressionContextMock gExpCtxt;
nebula::ObjectPool pool;
namespace nebula {

static AggregateExpression* expr = nullptr;

size_t aggFuncCall(size_t iters) {
    for (size_t i = 0; i < iters; ++i) {
        Expression::eval(expr, gExpCtxt);
    }
    return iters;
}

BENCHMARK_NAMED_PARAM_MULTI(aggFuncCall, AggregateExpressionBM)

}   // namespace nebula

using nebula::AggregateExpression;
using nebula::ConstantExpression;

int main(int argc, char** argv) {
    nebula::expr = (nebula::AggregateExpression::make(
        &pool, "avg", nebula::ConstantExpression::make(&pool, 2), false));
    nebula::AggData aggData;
    nebula::expr->setAggData(&aggData);

    folly::init(&argc, &argv, true);
    folly::runBenchmarks();

    return 0;
}
