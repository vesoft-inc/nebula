/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/ExecutionPlan.h"

#include "common/graph/Response.h"
#include "common/interface/gen-cpp2/graph_types.h"
#include "planner/plan/Logic.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/IdGenerator.h"

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(PlanNode* root) : id_(EPIdGenerator::instance().id()), root_(root) {}

ExecutionPlan::~ExecutionPlan() {}

uint64_t ExecutionPlan::makePlanNodeDesc(const PlanNode* node) {
    DCHECK(planDescription_ != nullptr);
    auto found = planDescription_->nodeIndexMap.find(node->id());
    if (found != planDescription_->nodeIndexMap.end()) {
        return found->second;
    }

    size_t planNodeDescPos = planDescription_->planNodeDescs.size();
    planDescription_->nodeIndexMap.emplace(node->id(), planNodeDescPos);
    planDescription_->planNodeDescs.emplace_back(std::move(*node->explain()));
    auto& planNodeDesc = planDescription_->planNodeDescs.back();
    planNodeDesc.profiles = std::make_unique<std::vector<ProfilingStats>>();

    if (node->kind() == PlanNode::Kind::kSelect) {
        auto select = static_cast<const Select*>(node);
        setPlanNodeDeps(select, &planNodeDesc);
        descBranchInfo(select->then(), true, select->id());
        descBranchInfo(select->otherwise(), false, select->id());
    } else if (node->kind() == PlanNode::Kind::kLoop) {
        auto loop = static_cast<const Loop*>(node);
        setPlanNodeDeps(loop, &planNodeDesc);
        descBranchInfo(loop->body(), true, loop->id());
    }

    for (size_t i = 0; i < node->numDeps(); ++i) {
        makePlanNodeDesc(node->dep(i));
    }

    return planNodeDescPos;
}

void ExecutionPlan::descBranchInfo(const PlanNode* node, bool isDoBranch, int64_t id) {
    auto pos = makePlanNodeDesc(node);
    auto info = std::make_unique<PlanNodeBranchInfo>();
    info->isDoBranch = isDoBranch;
    info->conditionNodeId = id;
    planDescription_->planNodeDescs[pos].branchInfo = std::move(info);
}

void ExecutionPlan::setPlanNodeDeps(const PlanNode* node, PlanNodeDescription* planNodeDesc) const {
    auto deps = std::make_unique<std::vector<int64_t>>();
    for (size_t i = 0; i < node->numDeps(); ++i) {
        deps->emplace_back(node->dep(i)->id());
    }
    planNodeDesc->dependencies = std::move(deps);
}

void ExecutionPlan::describe(PlanDescription* planDesc) {
    planDescription_ = DCHECK_NOTNULL(planDesc);
    planDescription_->optimize_time_in_us = optimizeTimeInUs_;
    planDescription_->format = explainFormat_;
    makePlanNodeDesc(root_);
}

void ExecutionPlan::addProfileStats(int64_t planNodeId, ProfilingStats&& profilingStats) {
    // return directly if not enable profile
    if (!planDescription_) return;

    auto found = planDescription_->nodeIndexMap.find(planNodeId);
    DCHECK(found != planDescription_->nodeIndexMap.end());
    auto idx = found->second;
    auto& planNodeDesc = planDescription_->planNodeDescs[idx];
    planNodeDesc.profiles->emplace_back(std::move(profilingStats));
}

}   // namespace graph
}   // namespace nebula
