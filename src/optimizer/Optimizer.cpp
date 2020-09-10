/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/Optimizer.h"

#include "context/QueryContext.h"
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

Optimizer::Optimizer(QueryContext *qctx, std::vector<const RuleSet *> ruleSets)
    : qctx_(qctx), ruleSets_(std::move(ruleSets)) {
    DCHECK(qctx != nullptr);
}

StatusOr<const PlanNode *> Optimizer::findBestPlan(PlanNode *root) {
    DCHECK(root != nullptr);
    rootNode_ = root;
    NG_RETURN_IF_ERROR(prepare());
    NG_RETURN_IF_ERROR(doExploration());
    return rootGroup_->getPlan();
}

Status Optimizer::prepare() {
    visitedNodes_.clear();
    rootGroup_ = convertToGroup(rootNode_);
    return Status::OK();
}

Status Optimizer::doExploration() {
    // TODO(yee): Apply all rules recursively, not only once round
    for (auto ruleSet : ruleSets_) {
        for (auto rule : ruleSet->rules()) {
            if (!rootGroup_->isExplored(rule)) {
                NG_RETURN_IF_ERROR(rootGroup_->explore(rule));
            }
        }
    }
    return Status::OK();
}

OptGroup *Optimizer::convertToGroup(PlanNode *node) {
    auto iter = visitedNodes_.find(node->id());
    if (iter != visitedNodes_.cend()) {
        return iter->second;
    }

    auto group = OptGroup::create(qctx_);
    auto groupExpr = group->makeGroupExpr(qctx_, node);

    switch (node->dependencies().size()) {
        case 0: {
            // Do nothing
            break;
        }
        case 1: {
            if (node->kind() == PlanNode::Kind::kSelect) {
                auto select = static_cast<Select *>(node);
                auto then = convertToGroup(const_cast<PlanNode *>(select->then()));
                groupExpr->addBody(then);
                auto otherwise = convertToGroup(const_cast<PlanNode *>(select->otherwise()));
                groupExpr->addBody(otherwise);
            } else if (node->kind() == PlanNode::Kind::kLoop) {
                auto loop = static_cast<Loop *>(node);
                auto body = convertToGroup(const_cast<PlanNode *>(loop->body()));
                groupExpr->addBody(body);
            }
            auto dep = static_cast<SingleDependencyNode *>(node)->dep();
            DCHECK(dep != nullptr);
            auto depGroup = convertToGroup(const_cast<graph::PlanNode *>(dep));
            groupExpr->dependsOn(depGroup);
            break;
        }
        case 2: {
            auto bNode = static_cast<BiInputNode *>(node);
            auto leftGroup = convertToGroup(const_cast<graph::PlanNode *>(bNode->left()));
            groupExpr->dependsOn(leftGroup);
            auto rightGroup = convertToGroup(const_cast<graph::PlanNode *>(bNode->right()));
            groupExpr->dependsOn(rightGroup);
            break;
        }
        default: {
            LOG(FATAL) << "Invalid number of plan node dependencies: "
                       << node->dependencies().size();
            break;
        }
    }
    visitedNodes_.emplace(node->id(), group);
    return group;
}

}   // namespace opt
}   // namespace nebula
