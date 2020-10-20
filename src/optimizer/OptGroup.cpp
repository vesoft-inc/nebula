/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptGroup.h"

#include <limits>

#include "context/QueryContext.h"
#include "optimizer/OptRule.h"
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

OptGroup *OptGroup::create(QueryContext *qctx) {
    return qctx->objPool()->add(new OptGroup(qctx));
}

OptGroup::OptGroup(QueryContext *qctx) noexcept : qctx_(qctx) {
    DCHECK(qctx != nullptr);
}

void OptGroup::addGroupNode(OptGroupNode *groupNode) {
    DCHECK(groupNode != nullptr);
    DCHECK(groupNode->group() == this);
    groupNodes_.emplace_back(groupNode);
}

OptGroupNode *OptGroup::makeGroupNode(QueryContext *qctx, PlanNode *node) {
    groupNodes_.emplace_back(OptGroupNode::create(qctx, node, this));
    return groupNodes_.back();
}

Status OptGroup::explore(const OptRule *rule) {
    if (isExplored(rule)) {
        return Status::OK();
    }
    setExplored(rule);

    for (auto iter = groupNodes_.begin(); iter != groupNodes_.end();) {
        auto groupNode = *iter;
        DCHECK(groupNode != nullptr);
        if (groupNode->isExplored(rule)) {
            ++iter;
            continue;
        }
        // Bottom to up exploration
        NG_RETURN_IF_ERROR(groupNode->explore(rule));

        // Find more equivalents
        auto status = rule->match(groupNode);
        if (!status.ok()) {
            ++iter;
            continue;
        }
        auto matched = std::move(status).value();
        auto resStatus = rule->transform(qctx_, matched);
        NG_RETURN_IF_ERROR(resStatus);
        auto result = std::move(resStatus).value();
        if (result.eraseAll) {
            groupNodes_.clear();
            for (auto ngn : result.newGroupNodes) {
                addGroupNode(ngn);
            }
            break;
        }

        if (!result.newGroupNodes.empty()) {
            for (auto ngn : result.newGroupNodes) {
                addGroupNode(ngn);
            }

            setUnexplored(rule);
        }

        if (result.eraseCurr) {
            iter = groupNodes_.erase(iter);
        } else {
            ++iter;
        }
    }

    return Status::OK();
}

Status OptGroup::exploreUntilMaxRound(const OptRule *rule) {
    auto maxRound = kMaxExplorationRound;
    while (!isExplored(rule)) {
        if (0 < maxRound--) {
            NG_RETURN_IF_ERROR(explore(rule));
        } else {
            setExplored(rule);
            break;
        }
    }
    return Status::OK();
}

std::pair<double, const OptGroupNode *> OptGroup::findMinCostGroupNode() const {
    double minCost = std::numeric_limits<double>::max();
    const OptGroupNode *minGroupNode = nullptr;
    for (auto &groupNode : groupNodes_) {
        double cost = groupNode->getCost();
        if (minCost > cost) {
            minCost = cost;
            minGroupNode = groupNode;
        }
    }
    return std::make_pair(minCost, minGroupNode);
}

double OptGroup::getCost() const {
    return findMinCostGroupNode().first;
}

const PlanNode *OptGroup::getPlan() const {
    const OptGroupNode *minGroupNode = findMinCostGroupNode().second;
    DCHECK(minGroupNode != nullptr);
    return minGroupNode->getPlan();
}

OptGroupNode *OptGroupNode::create(QueryContext *qctx, PlanNode *node, const OptGroup *group) {
    return qctx->objPool()->add(new OptGroupNode(node, group));
}

OptGroupNode::OptGroupNode(PlanNode *node, const OptGroup *group) noexcept
    : node_(node), group_(group) {
    DCHECK(node != nullptr);
    DCHECK(group != nullptr);
}

Status OptGroupNode::explore(const OptRule *rule) {
    if (isExplored(rule)) {
        return Status::OK();
    }
    setExplored(rule);

    for (auto dep : dependencies_) {
        DCHECK(dep != nullptr);
        NG_RETURN_IF_ERROR(dep->exploreUntilMaxRound(rule));
    }

    for (auto body : bodies_) {
        DCHECK(body != nullptr);
        NG_RETURN_IF_ERROR(body->exploreUntilMaxRound(rule));
    }
    return Status::OK();
}

double OptGroupNode::getCost() const {
    return node_->cost();
}

const PlanNode *OptGroupNode::getPlan() const {
    switch (node_->dependencies().size()) {
        case 0: {
            DCHECK(dependencies_.empty());
            break;
        }
        case 1: {
            DCHECK_EQ(dependencies_.size(), 1U);
            if (node_->kind() == PlanNode::Kind::kSelect) {
                DCHECK_EQ(bodies_.size(), 2U);
                auto select = static_cast<Select *>(node_);
                select->setIf(const_cast<PlanNode *>(bodies_[0]->getPlan()));
                select->setElse(const_cast<PlanNode *>(bodies_[1]->getPlan()));
            } else if (node_->kind() == PlanNode::Kind::kLoop) {
                DCHECK_EQ(bodies_.size(), 1U);
                auto loop = static_cast<Loop *>(node_);
                loop->setBody(const_cast<PlanNode *>(bodies_[0]->getPlan()));
            }
            auto singleDepNode = static_cast<SingleDependencyNode *>(node_);
            singleDepNode->dependsOn(dependencies_[0]->getPlan());
            break;
        }
        case 2: {
            DCHECK_EQ(dependencies_.size(), 2U);
            auto bNode = static_cast<BiInputNode *>(node_);
            bNode->setLeftDep(dependencies_[0]->getPlan());
            bNode->setRightDep(dependencies_[1]->getPlan());
            break;
        }
    }
    return node_;
}

}   // namespace opt
}   // namespace nebula
