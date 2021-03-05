/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/WhereClausePlanner.h"

#include "planner/Query.h"
#include "planner/match/MatchSolver.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> WhereClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kWhere) {
        return Status::Error("Not a valid context for WhereClausePlanner.");
    }
    auto* whereCtx = static_cast<WhereClauseContext*>(clauseCtx);

    SubPlan wherePlan;
    NG_RETURN_IF_ERROR(buildFilter(whereCtx, wherePlan));
    return wherePlan;
}

Status WhereClausePlanner::buildFilter(WhereClauseContext* wctx, SubPlan& subplan) {
    if (wctx->filter == nullptr) {
        return Status::OK();
    }

    auto newFilter = wctx->filter->clone();
    auto rewriter = [wctx](const Expression* expr) {
        return MatchSolver::doRewrite(*wctx->aliasesUsed, expr);
    };

    auto kind = newFilter->kind();
    if (kind == Expression::Kind::kLabel
        || kind == Expression::Kind::kLabelAttribute) {
        newFilter.reset(rewriter(newFilter.get()));
    } else {
        RewriteMatchLabelVisitor visitor(std::move(rewriter));
        newFilter->accept(&visitor);
    }

    auto* cond = wctx->qctx->objPool()->add(newFilter.release());
    subplan.root = Filter::make(wctx->qctx, nullptr, cond, true);
    subplan.tail = subplan.root;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
