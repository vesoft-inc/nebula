/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/UnwindClausePlanner.h"

#include "planner/Query.h"
#include "planner/match/MatchSolver.h"
#include "planner/match/OrderByClausePlanner.h"
#include "planner/match/PaginationPlanner.h"
#include "planner/match/SegmentsConnector.h"
#include "visitor/RewriteMatchLabelVisitor.h"

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
    auto* yields = new YieldColumns();
    uctx->qctx->objPool()->add(yields);
    std::vector<std::string> colNames;

    auto rewriter = [uctx](const Expression* expr) {
        return MatchSolver::doRewrite(*uctx->aliasesUsed, expr);
    };

    for (auto* col : uctx->yieldColumns->columns()) {
        auto kind = col->expr()->kind();
        YieldColumn* newColumn = nullptr;
        if (kind == Expression::Kind::kLabel || kind == Expression::Kind::kLabelAttribute) {
            newColumn = new YieldColumn(rewriter(col->expr()), new std::string(*col->alias()));
        } else {
            auto newExpr = col->expr()->clone();
            RewriteMatchLabelVisitor visitor(rewriter);
            newExpr->accept(&visitor);
            newColumn = new YieldColumn(newExpr.release());
        }
        yields->addColumn(newColumn);
        if (col->alias() != nullptr) {
            colNames.emplace_back(*col->alias());
        } else {
            return Status::Error("Expression in UNWIND must be aliased (use AS)");
        }
    }

    auto* unwind = Unwind::make(uctx->qctx, nullptr, yields);
    unwind->setColNames(std::move(colNames));
    subPlan.root = unwind;
    subPlan.tail = unwind;

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
