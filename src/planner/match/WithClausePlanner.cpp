/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/WithClausePlanner.h"

#include "planner/Query.h"
#include "planner/match/MatchSolver.h"
#include "planner/match/OrderByClausePlanner.h"
#include "planner/match/PaginationPlanner.h"
#include "planner/match/SegmentsConnector.h"
#include "planner/match/WhereClausePlanner.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {
StatusOr<SubPlan> WithClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
    if (clauseCtx->kind != CypherClauseKind::kWith) {
        return Status::Error("Not a valid context for WithClausePlanner.");
    }
    auto* withClauseCtx = static_cast<WithClauseContext*>(clauseCtx);

    SubPlan withPlan;
    NG_RETURN_IF_ERROR(buildWith(withClauseCtx, withPlan));
    return withPlan;
}

Status WithClausePlanner::buildWith(WithClauseContext* wctx, SubPlan& subPlan) {
    auto* yields = new YieldColumns();
    wctx->qctx->objPool()->add(yields);
    std::vector<std::string> colNames;
    PlanNode* current = nullptr;

    auto rewriter = [wctx](const Expression* expr) {
        return MatchSolver::doRewrite(*wctx->aliasesUsed, expr);
    };

    for (auto* col : wctx->yieldColumns->columns()) {
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
            colNames.emplace_back(col->expr()->toString());
        }
    }

    auto* project = Project::make(wctx->qctx, nullptr, yields);
    project->setColNames(std::move(colNames));
    subPlan.tail = project;
    current = project;

    if (wctx->distinct) {
        auto* dedup = Dedup::make(wctx->qctx, current);
        dedup->setInputVar(current->outputVar());
        dedup->setColNames(current->colNames());
        current = dedup;
    }

    subPlan.root = current;

    if (wctx->order != nullptr) {
        auto orderPlan = std::make_unique<OrderByClausePlanner>()->transform(wctx->order.get());
        NG_RETURN_IF_ERROR(orderPlan);
        auto plan = std::move(orderPlan).value();
        SegmentsConnector::addInput(plan.tail, subPlan.root, true);
        subPlan.root = plan.root;
    }

    if (wctx->pagination != nullptr &&
        (wctx->pagination->skip != 0 ||
         wctx->pagination->limit != std::numeric_limits<int64_t>::max())) {
        auto paginationPlan =
            std::make_unique<PaginationPlanner>()->transform(wctx->pagination.get());
        NG_RETURN_IF_ERROR(paginationPlan);
        auto plan = std::move(paginationPlan).value();
        SegmentsConnector::addInput(plan.tail, subPlan.root, true);
        subPlan.root = plan.root;
    }

    if (wctx->where != nullptr) {
        auto wherePlan = std::make_unique<WhereClausePlanner>()->transform(wctx->where.get());
        NG_RETURN_IF_ERROR(wherePlan);
        auto plan = std::move(wherePlan).value();
        SegmentsConnector::addInput(plan.tail, subPlan.root, true);
        subPlan.root = plan.root;
    }

    VLOG(1) << "with root: " << subPlan.root->outputVar()
            << " colNames: " << folly::join(",", subPlan.root->colNames());
    // TODO: Handle grouping

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
