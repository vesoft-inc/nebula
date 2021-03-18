/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/SegmentsConnector.h"
#include "planner/match/AddDependencyStrategy.h"
#include "planner/match/AddInputStrategy.h"
#include "planner/match/CartesianProductStrategy.h"

namespace nebula {
namespace graph {

StatusOr<SubPlan> SegmentsConnector::connectSegments(CypherClauseContextBase* leftCtx,
                                                     CypherClauseContextBase* rightCtx,
                                                     SubPlan& left,
                                                     SubPlan& right) {
    UNUSED(rightCtx);
    if (leftCtx->kind == CypherClauseKind::kReturn ||
        leftCtx->kind == CypherClauseKind::kWith ||
        leftCtx->kind == CypherClauseKind::kUnwind) {
        VLOG(1) << "left tail: " << left.tail->outputVar()
                << "right root: " << right.root->outputVar();
        addInput(left.tail, right.root);
        left.tail = right.tail;
        return left;
    }

    return Status::Error("Can not solve the connect strategy of the two subplan..");
}

PlanNode* SegmentsConnector::innerJoinSegments(QueryContext* qctx,
                                               const PlanNode* left,
                                               const PlanNode* right,
                                               InnerJoinStrategy::JoinPos leftPos,
                                               InnerJoinStrategy::JoinPos rightPos) {
    return std::make_unique<InnerJoinStrategy>(qctx)
                ->leftPos(leftPos)
                ->rightPos(rightPos)
                ->connect(left, right);
}

PlanNode* SegmentsConnector::cartesianProductSegments(QueryContext* qctx,
                                                      const PlanNode* left,
                                                      const PlanNode* right) {
    return std::make_unique<CartesianProductStrategy>(qctx)->connect(left, right);
}

void SegmentsConnector::addDependency(const PlanNode* left, const PlanNode* right) {
    std::make_unique<AddDependencyStrategy>()->connect(left, right);
}

void SegmentsConnector::addInput(const PlanNode* left, const PlanNode* right, bool copyColNames) {
    std::make_unique<AddInputStrategy>(copyColNames)->connect(left, right);
}
}  // namespace graph
}  // namespace nebula
