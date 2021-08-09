/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/planner/match/CartesianProductStrategy.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/plan/Algo.h"

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
