/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/logic/LoopExecutor.h"

#include <folly/String.h>

#include "common/time/ScopedTimer.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "interface/gen-cpp2/common_types.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

LoopExecutor::LoopExecutor(const PlanNode *node, QueryContext *qctx)
    : Executor("LoopExecutor", node, qctx) {}

folly::Future<Status> LoopExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *loopNode = asNode<Loop>(node());
  Expression *expr = loopNode->condition();
  QueryExpressionContext ctx(ectx_);

  auto value = expr->eval(ctx);
  VLOG(1) << "Loop condition: " << expr->toString() << " val: " << value;
  DCHECK(value.isBool());
  finally_ = !(value.isBool() && value.getBool());
  return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).build());
}

}  // namespace graph
}  // namespace nebula
