/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/InnerJoinStrategy.h"

#include "common/expression/AttributeExpression.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "planner/match/MatchSolver.h"

namespace nebula {
namespace graph {
PlanNode* InnerJoinStrategy::connect(const PlanNode* left, const PlanNode* right) {
    return joinDataSet(left, right);
}

PlanNode* InnerJoinStrategy::joinDataSet(const PlanNode* left, const PlanNode* right) {
    Expression* buildExpr = nullptr;
    if (leftPos_ == JoinPos::kStart) {
        auto& leftKey = left->colNames().front();
        buildExpr = MatchSolver::getStartVidInPath(qctx_, leftKey);
    } else {
        auto& leftKey = left->colNames().back();
        buildExpr = MatchSolver::getEndVidInPath(qctx_, leftKey);
    }

    Expression* probeExpr = nullptr;
    if (rightPos_ == JoinPos::kStart) {
        auto& rightKey = right->colNames().front();
        probeExpr = MatchSolver::getStartVidInPath(qctx_, rightKey);
    } else {
        auto& rightKey = right->colNames().back();
        probeExpr = MatchSolver::getEndVidInPath(qctx_, rightKey);
    }

    auto join = InnerJoin::make(qctx_,
                               const_cast<PlanNode*>(right),
                               {left->outputVar(), 0},
                               {right->outputVar(), 0},
                               {buildExpr},
                               {probeExpr});
    std::vector<std::string> colNames = left->colNames();
    const auto& rightColNames = right->colNames();
    colNames.insert(colNames.end(), rightColNames.begin(), rightColNames.end());
    join->setColNames(std::move(colNames));
    return join;
}
}  // namespace graph
}  // namespace nebula
