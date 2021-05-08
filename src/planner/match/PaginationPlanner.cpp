/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/PaginationPlanner.h"

#include "planner/plan/Query.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> PaginationPlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kPagination) {
        return Status::Error("Not a valid context for PaginationPlanner.");
    }
    auto* paginationCtx = static_cast<PaginationContext*>(clauseCtx);

    SubPlan paginationPlan;
    NG_RETURN_IF_ERROR(buildLimit(paginationCtx, paginationPlan));
    return paginationPlan;
}

Status PaginationPlanner::buildLimit(PaginationContext* pctx, SubPlan& subplan) {
    auto* currentRoot = subplan.root;
    auto* limit = Limit::make(pctx->qctx, currentRoot, pctx->skip, pctx->limit);
    subplan.root = limit;
    subplan.tail = limit;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
