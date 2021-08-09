/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/WhereClausePlanner.h"

#include "planner/plan/Query.h"
#include "planner/match/MatchSolver.h"
#include "visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> WhereClausePlanner::transform(CypherClauseContextBase* ctx) {
    if (ctx->kind != CypherClauseKind::kWhere) {
        return Status::Error("Not a valid context for WhereClausePlanner.");
    }

    auto* wctx = static_cast<WhereClauseContext*>(ctx);
    if (wctx->filter) {
        SubPlan wherePlan;
        auto* newFilter = MatchSolver::doRewrite(wctx->qctx, *wctx->aliasesUsed, wctx->filter);
        wherePlan.root = Filter::make(wctx->qctx, nullptr, newFilter, needStableFilter_);
        wherePlan.tail = wherePlan.root;

        return wherePlan;
    }

    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
