/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/AddDependencyStrategy.h"

namespace nebula {
namespace graph {
PlanNode* AddDependencyStrategy::connect(const PlanNode* left, const PlanNode* right) {
    auto* mutableLeft = const_cast<PlanNode*>(left);
    auto* siLeft = static_cast<SingleDependencyNode*>(mutableLeft);
    siLeft->dependsOn(const_cast<PlanNode*>(right));
    // siLeft->setColNames(right->colNames());
    return nullptr;
}
}  // namespace graph
}  // namespace nebula
