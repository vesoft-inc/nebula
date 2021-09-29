/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/query/LimitExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/ScopedTimer.h"

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
