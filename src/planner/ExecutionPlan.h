/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_EXECUTIONPLAN_H_
#define PLANNER_EXECUTIONPLAN_H_

#include <cstdint>
#include <memory>

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
