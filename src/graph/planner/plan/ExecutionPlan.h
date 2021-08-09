/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLAN_EXECUTIONPLAN_H_
#define PLANNER_PLAN_EXECUTIONPLAN_H_

#include <string>

namespace nebula {

struct ProfilingStats;
struct PlanDescription;
struct PlanNodeDescription;

namespace graph {

class PlanNode;

class ExecutionPlan final {
public:
    explicit ExecutionPlan(PlanNode* root = nullptr);
    ~ExecutionPlan();

    int64_t id() const {
        return id_;
    }

    void setRoot(PlanNode* root) {
        root_ = root;
    }

    PlanNode* root() const {
        return root_;
    }

    int32_t* optimizeTimeInUs() {
        return &optimizeTimeInUs_;
    }

    void addProfileStats(int64_t planNodeId, ProfilingStats&& profilingStats);

    void describe(PlanDescription* planDesc);

    void setExplainFormat(const std::string& format) {
        explainFormat_ = format;
    }

private:
    uint64_t makePlanNodeDesc(const PlanNode* node);
    void descBranchInfo(const PlanNode* node, bool isDoBranch, int64_t id);
    void setPlanNodeDeps(const PlanNode* dep, PlanNodeDescription* planNodeDesc) const;

    int32_t optimizeTimeInUs_{0};
    int64_t id_{-1};
    PlanNode* root_{nullptr};
    // plan description for explain and profile query
    PlanDescription* planDescription_{nullptr};
    std::string explainFormat_;
};

}   // namespace graph
}   // namespace nebula

#endif
