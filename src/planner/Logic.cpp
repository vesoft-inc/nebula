/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Logic.h"

#include "common/interface/gen-cpp2/graph_types.h"

namespace nebula {
namespace graph {

std::unique_ptr<PlanNodeDescription> BinarySelect::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("condition", condition_ ? condition_->toString() : "", desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> Loop::explain() const {
    auto desc = BinarySelect::explain();
    addDescription("loopBody", std::to_string(body_->id()), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> Select::explain() const {
    auto desc = BinarySelect::explain();
    addDescription("thenBody", std::to_string(if_->id()), desc.get());
    addDescription("elseBody", std::to_string(else_->id()), desc.get());
    return desc;
}

}   // namespace graph
}   // namespace nebula
