/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include <folly/String.h>

#include "common/interface/gen-cpp2/graph_types.h"
#include "exec/Executor.h"
#include "planner/Logic.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "scheduler/Scheduler.h"
#include "util/IdGenerator.h"
#include "util/ObjectPool.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

ExecutionPlan::ExecutionPlan(ObjectPool* objectPool)
    : id_(EPIdGenerator::instance().id()),
      objPool_(objectPool),
      nodeIdGen_(std::make_unique<IdGenerator>(0)) {}

ExecutionPlan::~ExecutionPlan() {}

PlanNode* ExecutionPlan::addPlanNode(PlanNode* node) {
    node->setId(nodeIdGen_->id());
    return objPool_->add(node);
}

static size_t makePlanNodeDesc(const PlanNode* node, cpp2::PlanDescription* planDesc) {
    auto found = planDesc->node_index_map.find(node->id());
    if (found != planDesc->node_index_map.end()) {
        return found->second;
    }

    size_t planNodeDescPos = planDesc->plan_node_descs.size();
    planDesc->node_index_map.emplace(node->id(), planNodeDescPos);
    planDesc->plan_node_descs.emplace_back(cpp2::PlanNodeDescription{});
    auto& planNodeDesc = planDesc->plan_node_descs.back();

    planNodeDesc.set_id(node->id());
    planNodeDesc.set_name(PlanNode::toString(node->kind()));
    planNodeDesc.set_output_var(node->varName());
    cpp2::Pair p;
    p.set_key("description");
    p.set_value(node->explain());
    planNodeDesc.set_description({std::move(p)});

    switch (node->kind()) {
        case PlanNode::Kind::kStart: {
            break;
        }
        case PlanNode::Kind::kUnion:
        case PlanNode::Kind::kIntersect:
        case PlanNode::Kind::kMinus: {
            auto bNode = static_cast<const BiInputNode*>(node);
            planNodeDesc.set_dependencies({bNode->left()->id(), bNode->right()->id()});
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
            planNodeDesc.set_dependencies({singleDepNode->dep()->id()});
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
