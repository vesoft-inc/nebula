/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/logic/SelectExecutor.h"

#include "context/QueryExpressionContext.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

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
    return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).finish());
}

}   // namespace graph
}   // namespace nebula
