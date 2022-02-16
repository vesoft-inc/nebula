/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/logic/SelectExecutor.h"

#include <memory>   // for allocator
#include <utility>  // for move

#include "common/base/Status.h"                    // for Status
#include "common/datatypes/Value.h"                // for Value
#include "common/expression/Expression.h"          // for Expression
#include "graph/context/Iterator.h"                // for Iterator, Iterator...
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder
#include "graph/planner/plan/Logic.h"              // for Select

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;

class PlanNode;
class QueryContext;

SelectExecutor::SelectExecutor(const PlanNode* node, QueryContext* qctx)
    : Executor("SelectExecutor", node, qctx) {}

folly::Future<Status> SelectExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* select = asNode<Select>(node());
  auto* expr = select->condition();
  QueryExpressionContext ctx(ectx_);
  auto value = expr->eval(ctx);
  DCHECK(value.isBool());
  return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).build());
}

}  // namespace graph
}  // namespace nebula
