/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/PlanNode.h"
#include "planner/ExecutionPlan.h"

namespace nebula {
namespace graph {
PlanNode::PlanNode(ExecutionPlan* plan) {
    plan_ = plan;
    plan_->addPlanNode(this);
}
}  // namespace graph
}  // namespace nebula
