/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_EXECUTIONPLAN_H_
#define PLANNER_EXECUTIONPLAN_H_

#include <cstdint>

namespace nebula {

struct PlanDescription;

namespace graph {

class PlanNode;

class ExecutionPlan final {
public:
    explicit ExecutionPlan(PlanNode* root = nullptr);
    ~ExecutionPlan();

    void setRoot(PlanNode* root) {
        root_ = root;
    }

    int64_t id() const {
        return id_;
    }

    PlanNode* root() const {
        return root_;
    }

    int32_t* optimizeTimeInUs() {
        return &optimizeTimeInUs_;
    }

    void fillPlanDescription(PlanDescription* planDesc) const;

private:
    int32_t optimizeTimeInUs_{0};
    int64_t id_{-1};
    PlanNode* root_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif
