/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include "common/graph/Response.h"
#include "planner/Logic.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "util/IdGenerator.h"

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(PlanNode* root) : id_(EPIdGenerator::instance().id()), root_(root) {}

ExecutionPlan::~ExecutionPlan() {}

static size_t makePlanNodeDesc(const PlanNode* node, PlanDescription* planDesc) {
    auto found = planDesc->nodeIndexMap.find(node->id());
    if (found != planDesc->nodeIndexMap.end()) {
        return found->second;
    }

    size_t planNodeDescPos = planDesc->planNodeDescs.size();
    planDesc->nodeIndexMap.emplace(node->id(), planNodeDescPos);
    planDesc->planNodeDescs.emplace_back(std::move(*node->explain()));
    auto& planNodeDesc = planDesc->planNodeDescs.back();

    switch (node->kind()) {
        case PlanNode::Kind::kStart: {
            break;
        }
        case PlanNode::Kind::kUnion:
        case PlanNode::Kind::kIntersect:
        case PlanNode::Kind::kMinus:
        case PlanNode::Kind::kConjunctPath: {
            auto bNode = static_cast<const BiInputNode*>(node);
            makePlanNodeDesc(bNode->left(), planDesc);
            makePlanNodeDesc(bNode->right(), planDesc);
            break;
        }
        case PlanNode::Kind::kSelect: {
            auto select = static_cast<const Select*>(node);
            planNodeDesc.dependencies.reset(new std::vector<int64_t>{select->dep()->id()});
            auto thenPos = makePlanNodeDesc(select->then(), planDesc);
            PlanNodeBranchInfo thenInfo;
            thenInfo.isDoBranch = true;
            thenInfo.conditionNodeId = select->id();
            planDesc->planNodeDescs[thenPos].branchInfo =
                std::make_unique<PlanNodeBranchInfo>(std::move(thenInfo));
            auto otherwisePos = makePlanNodeDesc(select->otherwise(), planDesc);
            PlanNodeBranchInfo elseInfo;
            elseInfo.isDoBranch = false;
            elseInfo.conditionNodeId = select->id();
            planDesc->planNodeDescs[otherwisePos].branchInfo =
                std::make_unique<PlanNodeBranchInfo>(std::move(elseInfo));
            makePlanNodeDesc(select->dep(), planDesc);
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<const Loop*>(node);
            planNodeDesc.dependencies.reset(new std::vector<int64_t>{loop->dep()->id()});
            auto bodyPos = makePlanNodeDesc(loop->body(), planDesc);
            PlanNodeBranchInfo info;
            info.isDoBranch = true;
            info.conditionNodeId = loop->id();
            planDesc->planNodeDescs[bodyPos].branchInfo =
                std::make_unique<PlanNodeBranchInfo>(std::move(info));
            makePlanNodeDesc(loop->dep(), planDesc);
            break;
        }
        default: {
            // Other plan nodes have single dependency
            DCHECK_EQ(node->dependencies().size(), 1U);
            auto singleDepNode = static_cast<const SingleDependencyNode*>(node);
            makePlanNodeDesc(singleDepNode->dep(), planDesc);
            break;
        }
    }
    return planNodeDescPos;
}

void ExecutionPlan::fillPlanDescription(PlanDescription* planDesc) const {
    DCHECK(planDesc != nullptr);
    planDesc->optimize_time_in_us = optimizeTimeInUs_;
    makePlanNodeDesc(root_, planDesc);
}

}   // namespace graph
}   // namespace nebula
