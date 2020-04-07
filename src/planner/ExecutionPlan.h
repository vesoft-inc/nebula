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
        nodeIdGen_ = std::make_unique<IdGenerator>(0);
    }

    ~ExecutionPlan() {
        for (auto* n : nodes_) {
            delete n;
        }
    }

    void setRoot(PlanNode* root) {
        root_ = root;
    }

    PlanNode* addPlanNode(PlanNode* node) {
        node->setId(nodeIdGen_->id());
        nodes_.emplace_back(node);
        return node;
    }

private:
    int64_t                                 id_{IdGenerator::INVALID_ID};
    PlanNode*                               root_{nullptr};
    std::vector<PlanNode*>                  nodes_;
    std::unique_ptr<IdGenerator>            nodeIdGen_;
};
}  // namespace graph
}  // namespace nebula
#endif
