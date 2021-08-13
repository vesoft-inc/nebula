/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/logic/LoopExecutor.h"

#include <folly/String.h>

#include "graph/context/QueryExpressionContext.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ScopedTimer.h"
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
  return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).finish());
}

}  // namespace graph
}  // namespace nebula
