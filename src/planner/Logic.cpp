/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Logic.h"

#include "common/interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

std::unique_ptr<cpp2::PlanNodeDescription> BinarySelect::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("condition", condition_ ? condition_->toString() : "", desc.get());
    return desc;
}

Loop::Loop(ExecutionPlan* plan, PlanNode* input, PlanNode* body, Expression* condition)
    : BinarySelect(plan, Kind::kLoop, input, condition), body_(body) {}

}   // namespace graph
}   // namespace nebula
