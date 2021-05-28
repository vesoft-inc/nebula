/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>
#include <memory>
#include "common/expression/ConstantExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/test/ExpressionContextMock.h"

nebula::ExpressionContextMock gExpCtxt;

namespace nebula {

static std::unique_ptr<FunctionCallExpression> expr = nullptr;

size_t funcCall(size_t iters) {
    for (size_t i = 0; i < iters; ++i) {
        Value eval = Expression::eval(expr.get(), gExpCtxt);
        folly::doNotOptimizeAway(eval);
    }
    return iters;
}

BENCHMARK_NAMED_PARAM_MULTI(funcCall, FunctionCallBM)

}   // namespace nebula

int main(int argc, char** argv) {
    nebula::ArgumentList* args = new nebula::ArgumentList();
    args->addArgument(std::make_unique<nebula::ConstantExpression>(1));
    nebula::expr.reset(new nebula::FunctionCallExpression("abs", args));

    folly::init(&argc, &argv, true);
    folly::runBenchmarks();

    return 0;
}
