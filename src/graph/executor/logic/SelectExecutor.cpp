/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/logic/SelectExecutor.h"

#include "common/time/ScopedTimer.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

SelectExecutor::SelectExecutor(const PlanNode* node, QueryContext* qctx)
    : Executor("SelectExecutor", node, qctx) {}

folly::Future<Status> SelectExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* select = asNode<Select>(node());
  auto* expr = select->condition();
  QueryExpressionContext ctx(ectx_);
  auto value = expr->eval(ctx);
  DCHECK(value.isBool());
  condition_ = (value.isBool() && value.getBool());
  return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).build());
}

}  // namespace graph
}  // namespace nebula
