/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/LimitExecutor.h"

#include <type_traits>  // for remove_reference<>...
#include <utility>      // for move

#include "common/base/Status.h"                    // for Status
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder, Result
#include "graph/planner/plan/Query.h"              // for Limit

namespace nebula {
namespace graph {

folly::Future<Status> LimitExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* limit = asNode<Limit>(node());
  Result result = ectx_->getResult(limit->inputVar());
  auto* iter = result.iterRef();
  ResultBuilder builder;
  builder.value(result.valuePtr());
  auto offset = limit->offset();
  QueryExpressionContext qec(ectx_);
  auto count = limit->count(qec);
  iter->select(offset, count);
  builder.iter(std::move(result).iter());
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
