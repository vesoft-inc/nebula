/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/UnwindClausePlanner.h"

#include "planner/plan/Query.h"
#include "planner/match/MatchSolver.h"
#include "planner/match/OrderByClausePlanner.h"
#include "planner/match/PaginationPlanner.h"
#include "planner/match/SegmentsConnector.h"
#include "visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> UnwindClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kUnwind) {
        return Status::Error("Not a valid context for UnwindClausePlanner.");
    }
    auto* unwindClauseCtx = static_cast<UnwindClauseContext*>(clauseCtx);

    SubPlan unwindPlan;
    NG_RETURN_IF_ERROR(buildUnwind(unwindClauseCtx, unwindPlan));
    return unwindPlan;
}

Status UnwindClausePlanner::buildUnwind(UnwindClauseContext* uctx, SubPlan& subPlan) {
    auto* newUnwindExpr = MatchSolver::doRewrite(uctx->qctx, *uctx->aliasesUsed, uctx->unwindExpr);
    auto* unwind = Unwind::make(uctx->qctx, nullptr, newUnwindExpr, uctx->alias);
    unwind->setColNames({uctx->alias});
    subPlan.root = unwind;
    subPlan.tail = unwind;

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
