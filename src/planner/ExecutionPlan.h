/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_EXECUTIONPLAN_H_
#define PLANNER_EXECUTIONPLAN_H_

#include "planner/PlanNode.h"
#include "planner/IdGenerator.h"

namespace nebula {
namespace graph {
class ExecutionPlan final {
public:
    ExecutionPlan() {
        id_ = EPIdGenerator::instance().id();
    }

    void setRoot(PlanNode* root) {
        root_ = root;
    }

    PlanNode* addPlanNode(std::unique_ptr<PlanNode>&& node) {
        node->setId(nodeIdGen_.id());
        auto* tmp = node.get();
        nodes_.emplace_back(std::move(node));
        return tmp;
    }

private:
    int64_t                                 id_{IdGenerator::INVALID_ID};
    PlanNode*                               root_;
    std::vector<std::unique_ptr<PlanNode>>  nodes_;
    IdGenerator                             nodeIdGen_;
};
}  // namespace graph
}  // namespace nebula
#endif
