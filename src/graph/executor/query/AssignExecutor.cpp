// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/AssignExecutor.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> AssignExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* assign = asNode<Assign>(node());
  auto& items = assign->items();
  QueryExpressionContext ctx(ectx_);

  for (const auto& item : items) {
    auto varName = item.first;
    auto& valueExpr = item.second;
    auto value = valueExpr->eval(ctx);
    if (value.isDataSet()) {
      ectx_->setResult(varName, ResultBuilder().value(std::move(value)).build());
    } else {
      ectx_->setValue(varName, std::move(value));
    }
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
