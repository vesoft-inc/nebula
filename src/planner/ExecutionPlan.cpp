/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include "common/interface/gen-cpp2/graph_types.h"
#include "planner/Logic.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "util/IdGenerator.h"

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan() : id_(EPIdGenerator::instance().id()) {}

ExecutionPlan::~ExecutionPlan() {}

static size_t makePlanNodeDesc(const PlanNode* node, cpp2::PlanDescription* planDesc) {
    auto found = planDesc->node_index_map.find(node->id());
    if (found != planDesc->node_index_map.end()) {
        return found->second;
    }

    size_t planNodeDescPos = planDesc->plan_node_descs.size();
    planDesc->node_index_map.emplace(node->id(), planNodeDescPos);
    planDesc->plan_node_descs.emplace_back(std::move(*node->explain()));
    auto& planNodeDesc = planDesc->plan_node_descs.back();

    switch (node->kind()) {
        case PlanNode::Kind::kStart: {
            break;
        }
        case PlanNode::Kind::kUnion:
        case PlanNode::Kind::kIntersect:
        case PlanNode::Kind::kMinus: {
            auto bNode = static_cast<const BiInputNode*>(node);
            makePlanNodeDesc(bNode->left(), planDesc);
            makePlanNodeDesc(bNode->right(), planDesc);
            break;
        }
        case PlanNode::Kind::kSelect: {
            auto select = static_cast<const Select*>(node);
            planNodeDesc.set_dependencies({select->dep()->id()});
            auto thenPos = makePlanNodeDesc(select->then(), planDesc);
            cpp2::PlanNodeBranchInfo thenInfo;
            thenInfo.set_is_do_branch(true);
            thenInfo.set_condition_node_id(select->id());
            planDesc->plan_node_descs[thenPos].set_branch_info(std::move(thenInfo));
            auto otherwisePos = makePlanNodeDesc(select->otherwise(), planDesc);
            cpp2::PlanNodeBranchInfo elseInfo;
            elseInfo.set_is_do_branch(false);
            elseInfo.set_condition_node_id(select->id());
            planDesc->plan_node_descs[otherwisePos].set_branch_info(std::move(elseInfo));
            makePlanNodeDesc(select->dep(), planDesc);
            break;
        }
        case PlanNode::Kind::kLoop: {
            auto loop = static_cast<const Loop*>(node);
            planNodeDesc.set_dependencies({loop->dep()->id()});
            auto bodyPos = makePlanNodeDesc(loop->body(), planDesc);
            cpp2::PlanNodeBranchInfo info;
            info.set_is_do_branch(true);
            info.set_condition_node_id(loop->id());
            planDesc->plan_node_descs[bodyPos].set_branch_info(std::move(info));
            makePlanNodeDesc(loop->dep(), planDesc);
            break;
        }
        default: {
            // Other plan nodes have single dependency
            auto singleDepNode = static_cast<const SingleDependencyNode*>(node);
            makePlanNodeDesc(singleDepNode->dep(), planDesc);
            break;
        }
    }
    return planNodeDescPos;
}

void ExecutionPlan::fillPlanDescription(cpp2::PlanDescription* planDesc) const {
    DCHECK(planDesc != nullptr);
    makePlanNodeDesc(root_, planDesc);
}

}   // namespace graph
}   // namespace nebula
