/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/OrderByClausePlanner.h"

#include "planner/plan/Query.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> OrderByClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kOrderBy) {
        return Status::Error("Not a valid context for OrderByClausePlanner.");
    }
    auto* sortCtx = static_cast<OrderByClauseContext*>(clauseCtx);

    SubPlan sortPlan;
    NG_RETURN_IF_ERROR(buildSort(sortCtx, sortPlan));
    return sortPlan;
}

Status OrderByClausePlanner::buildSort(OrderByClauseContext* octx, SubPlan& subplan) {
    auto* currentRoot = subplan.root;
    auto* sort = Sort::make(octx->qctx, currentRoot, octx->indexedOrderFactors);
    subplan.root = sort;
    subplan.tail = sort;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
