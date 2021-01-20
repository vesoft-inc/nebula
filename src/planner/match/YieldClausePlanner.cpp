/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/YieldClausePlanner.h"

#include "planner/Query.h"
#include "visitor/RewriteMatchLabelVisitor.h"
#include "planner/match/MatchSolver.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> YieldClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kYield) {
        return Status::Error("Not a valid context for YieldClausePlanner.");
    }
    auto* yieldCtx = static_cast<YieldClauseContext*>(clauseCtx);

    SubPlan yieldPlan;
    NG_RETURN_IF_ERROR(buildYield(yieldCtx, yieldPlan));
    return yieldPlan;
}

void YieldClausePlanner::rewriteYieldColumns(const YieldClauseContext* yctx,
                                             const YieldColumns* yields,
                                             YieldColumns* newYields) {
    auto* aliasesUsed = yctx->aliasesUsed;
    auto rewriter = [aliasesUsed](const Expression* expr) {
        return MatchSolver::doRewrite(*aliasesUsed, expr);
    };

    for (auto* col : yields->columns()) {
        auto colExpr = col->expr();
        auto kind = colExpr->kind();
        YieldColumn* newColumn = nullptr;
        if (kind == Expression::Kind::kLabel || kind == Expression::Kind::kLabelAttribute) {
            newColumn = new YieldColumn(rewriter(colExpr));
        } else {
            auto newExpr = colExpr->clone();
            RewriteMatchLabelVisitor visitor(rewriter);
            newExpr->accept(&visitor);
            newColumn = new YieldColumn(newExpr.release());
        }
        newYields->addColumn(newColumn);
    }
}

void YieldClausePlanner::rewriteGroupExprs(const YieldClauseContext* yctx,
                                           const std::vector<Expression*>* exprs,
                                           std::vector<Expression*>* newExprs) {
    auto* aliasesUsed = yctx->aliasesUsed;
    auto rewriter = [aliasesUsed](const Expression* expr) {
        return MatchSolver::doRewrite(*aliasesUsed, expr);
    };

    for (auto* expr : *exprs) {
        auto kind = expr->kind();
        if (kind == Expression::Kind::kLabel || kind == Expression::Kind::kLabelAttribute) {
            auto* newExpr = yctx->qctx->objPool()->add(rewriter(expr));
            newExprs->emplace_back(newExpr);
        } else {
            auto* newExpr = yctx->qctx->objPool()->add(expr->clone().release());
            RewriteMatchLabelVisitor visitor(rewriter);
            newExpr->accept(&visitor);
            newExprs->emplace_back(newExpr);
        }
    }
}

Status YieldClausePlanner::buildYield(YieldClauseContext* yctx, SubPlan& subplan) {
    auto* currentRoot = subplan.root;
    DCHECK(!currentRoot);
    auto* newProjCols = yctx->qctx->objPool()->add(new YieldColumns());
    rewriteYieldColumns(yctx, yctx->projCols_, newProjCols);
    if (!yctx->hasAgg_) {
        auto* project = Project::make(yctx->qctx,
                                      currentRoot,
                                      newProjCols);
        project->setColNames(std::move(yctx->projOutputColumnNames_));
        subplan.root = project;
        subplan.tail = project;
    } else {
        std::vector<Expression*> newGroupKeys;
        std::vector<Expression*> newGroupItems;
        rewriteGroupExprs(yctx, &yctx->groupKeys_, &newGroupKeys);
        rewriteGroupExprs(yctx, &yctx->groupItems_, &newGroupItems);

        auto* agg = Aggregate::make(yctx->qctx,
                                    currentRoot,
                                    std::move(newGroupKeys),
                                    std::move(newGroupItems));
        agg->setColNames(std::vector<std::string>(yctx->aggOutputColumnNames_));
        if (yctx->needGenProject_) {
            auto *project = Project::make(yctx->qctx, agg, newProjCols);
            project->setInputVar(agg->outputVar());
            project->setColNames(std::move(yctx->projOutputColumnNames_));
            subplan.root = project;
        } else {
            subplan.root = agg;
        }
        subplan.tail = agg;
    }

    if (yctx->distinct) {
        auto root = subplan.root;
        auto* dedup = Dedup::make(yctx->qctx, root);
        dedup->setInputVar(root->outputVar());
        dedup->setColNames(root->colNames());
        subplan.root = dedup;
    }

    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
