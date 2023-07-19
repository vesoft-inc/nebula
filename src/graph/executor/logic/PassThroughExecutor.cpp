// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/logic/PassThroughExecutor.h"

#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
folly::Future<Status> PassThroughExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  const auto &result = ectx_->getResult(node()->outputVar());
  auto iter = result.iter();
  if (!iter->isDefaultIter() && !iter->empty()) {
    // Return directly if this pass through output result is not empty
    return Status::OK();
  }

  DataSet ds;
  ds.colNames = node()->colNames();
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
