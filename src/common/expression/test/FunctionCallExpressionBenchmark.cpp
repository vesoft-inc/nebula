/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>      // for addBenchmark
#include <folly/BenchmarkUtil.h>  // for doNotOptim...
#include <folly/init/Init.h>      // for init
#include <stddef.h>               // for size_t

#include <memory>  // for allocator

#include "common/base/ObjectPool.h"                        // for ObjectPool
#include "common/datatypes/Value.h"                        // for Value
#include "common/expression/ConstantExpression.h"          // for ConstantEx...
#include "common/expression/Expression.h"                  // for Expression
#include "common/expression/FunctionCallExpression.h"      // for ArgumentList
#include "common/expression/test/ExpressionContextMock.h"  // for Expression...

nebula::ExpressionContextMock gExpCtxt;
nebula::ObjectPool pool;
namespace nebula {

static FunctionCallExpression* expr = nullptr;

size_t funcCall(size_t iters) {
  for (size_t i = 0; i < iters; ++i) {
    Value eval = Expression::eval(expr, gExpCtxt);
    folly::doNotOptimizeAway(eval);
  }
  return iters;
}

BENCHMARK_NAMED_PARAM_MULTI(funcCall, FunctionCallBM)

}  // namespace nebula

int main(int argc, char** argv) {
  nebula::ArgumentList* args = nebula::ArgumentList::make(&pool);
  args->addArgument(nebula::ConstantExpression::make(&pool, 1));
  nebula::expr = nebula::FunctionCallExpression::make(&pool, "abs", args);

  folly::init(&argc, &argv, true);
  folly::runBenchmarks();

  return 0;
}
