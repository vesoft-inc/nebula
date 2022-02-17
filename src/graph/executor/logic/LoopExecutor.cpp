/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/logic/LoopExecutor.h"

#include <folly/String.h>  // for stringPrintf

#include <string>   // for allocator, operator<<
#include <utility>  // for move

#include "common/base/Status.h"            // for Status
#include "common/datatypes/Value.h"        // for operator<<, Value
#include "common/expression/Expression.h"  // for Expression
#include "graph/context/Iterator.h"        // for Iterator, Iterator...
#include "graph/context/Result.h"          // for ResultBuilder
#include "graph/planner/plan/Logic.h"      // for Loop

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;

class PlanNode;
class QueryContext;
}  // namespace graph
}  // namespace nebula

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
