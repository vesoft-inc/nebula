/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/Optimizer.h"

#include "context/QueryContext.h"
#include "optimizer/OptContext.h"
#include "optimizer/OptGroup.h"
#include "optimizer/OptRule.h"
#include "planner/ExecutionPlan.h"
#include "planner/Logic.h"
#include "planner/PlanNode.h"

using nebula::graph::BiInputNode;
using nebula::graph::Loop;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Select;
using nebula::graph::SingleDependencyNode;

namespace nebula {
namespace opt {

Optimizer::Optimizer(std::vector<const RuleSet *> ruleSets) : ruleSets_(std::move(ruleSets)) {}

StatusOr<const PlanNode *> Optimizer::findBestPlan(QueryContext *qctx) {
    DCHECK(qctx != nullptr);
    auto optCtx = std::make_unique<OptContext>(qctx);

    auto root = qctx->plan()->root();
    auto status = prepare(optCtx.get(), root);
    NG_RETURN_IF_ERROR(status);
    auto rootGroup = std::move(status).value();

    NG_RETURN_IF_ERROR(doExploration(rootGroup));
    return rootGroup->getPlan();
}

StatusOr<OptGroup *> Optimizer::prepare(OptContext *ctx, PlanNode *root) {
    std::unordered_map<int64_t, OptGroup *> visited;
    return convertToGroup(ctx, root, &visited);
}

Status Optimizer::doExploration(OptGroup *rootGroup) {
    for (auto ruleSet : ruleSets_) {
        for (auto rule : ruleSet->rules()) {
            NG_RETURN_IF_ERROR(rootGroup->exploreUntilMaxRound(rule));
        }
    }
    return Status::OK();
}

OptGroup *Optimizer::convertToGroup(OptContext *ctx,
                                    PlanNode *node,
                                    std::unordered_map<int64_t, OptGroup *> *visited) {
    auto iter = visited->find(node->id());
    if (iter != visited->cend()) {
        return iter->second;
    }

    auto group = OptGroup::create(ctx);
    auto groupNode = group->makeGroupNode(node);

    switch (node->dependencies().size()) {
        case 0: {
            // Do nothing
            break;
        }
        case 1: {
            if (node->kind() == PlanNode::Kind::kSelect) {
                auto select = static_cast<Select *>(node);
                auto then = convertToGroup(ctx, const_cast<PlanNode *>(select->then()), visited);
                groupNode->addBody(then);
                auto otherNode = const_cast<PlanNode *>(select->otherwise());
                auto otherwise = convertToGroup(ctx, otherNode, visited);
                groupNode->addBody(otherwise);
            } else if (node->kind() == PlanNode::Kind::kLoop) {
                auto loop = static_cast<Loop *>(node);
                auto body = convertToGroup(ctx, const_cast<PlanNode *>(loop->body()), visited);
                groupNode->addBody(body);
            }
            auto dep = static_cast<SingleDependencyNode *>(node)->dep();
            DCHECK(dep != nullptr);
            auto depGroup = convertToGroup(ctx, const_cast<graph::PlanNode *>(dep), visited);
            groupNode->dependsOn(depGroup);
            break;
        }
        case 2: {
            auto bNode = static_cast<BiInputNode *>(node);
            auto leftNode = const_cast<graph::PlanNode *>(bNode->left());
            auto leftGroup = convertToGroup(ctx, leftNode, visited);
            groupNode->dependsOn(leftGroup);
            auto rightNode = const_cast<graph::PlanNode *>(bNode->right());
            auto rightGroup = convertToGroup(ctx, rightNode, visited);
            groupNode->dependsOn(rightGroup);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid number of plan node dependencies: "
                       << node->dependencies().size();
            break;
        }
    }
    visited->emplace(node->id(), group);
    return group;
}

}   // namespace opt
}   // namespace nebula
