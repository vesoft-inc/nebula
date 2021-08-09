/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/CartesianProductStrategy.h"

#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"
#include "planner/match/MatchSolver.h"
#include "planner/plan/Algo.h"

namespace nebula {
namespace graph {
PlanNode* CartesianProductStrategy::connect(const PlanNode* left, const PlanNode* right) {
    return joinDataSet(left, right);
}

PlanNode* CartesianProductStrategy::joinDataSet(const PlanNode* left, const PlanNode* right) {
    DCHECK(left->outputVar() != right->outputVar());

    auto* cartesianProduct = CartesianProduct::make(qctx_, nullptr);
    cartesianProduct->addVar(left->outputVar());
    cartesianProduct->addVar(right->outputVar());

    return cartesianProduct;
}
}  // namespace graph
}  // namespace nebula
