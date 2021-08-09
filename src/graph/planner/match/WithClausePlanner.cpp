/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/WithClausePlanner.h"

#include "planner/plan/Query.h"
#include "planner/match/OrderByClausePlanner.h"
#include "planner/match/PaginationPlanner.h"
#include "planner/match/YieldClausePlanner.h"
#include "planner/match/SegmentsConnector.h"
#include "planner/match/WhereClausePlanner.h"

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
    auto yieldPlan = std::make_unique<YieldClausePlanner>()->transform(wctx->yield.get());
    NG_RETURN_IF_ERROR(yieldPlan);
    auto yieldplan = std::move(yieldPlan).value();
    subPlan.tail = yieldplan.tail;
    subPlan.root = yieldplan.root;

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
        bool needStableFilter = wctx->order != nullptr;
        auto wherePlan =
            std::make_unique<WhereClausePlanner>(needStableFilter)->transform(wctx->where.get());
        NG_RETURN_IF_ERROR(wherePlan);
        auto plan = std::move(wherePlan).value();
        SegmentsConnector::addInput(plan.tail, subPlan.root, true);
        subPlan.root = plan.root;
    }

    VLOG(1) << "with root: " << subPlan.root->outputVar()
            << " colNames: " << folly::join(",", subPlan.root->colNames());

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
