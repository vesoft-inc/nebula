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

void OptGroup::addGroupExpr(OptGroupExpr *groupExpr) {
    DCHECK(groupExpr != nullptr);
    DCHECK(groupExpr->group() == this);
    groupExprs_.emplace_back(groupExpr);
}

OptGroupExpr *OptGroup::makeGroupExpr(QueryContext *qctx, PlanNode *node) {
    groupExprs_.emplace_back(OptGroupExpr::create(qctx, node, this));
    return groupExprs_.back();
}

Status OptGroup::explore(const OptRule *rule) {
    if (isExplored(rule)) {
        return Status::OK();
    }
    setExplored(rule);

    for (auto iter = groupExprs_.begin(); iter != groupExprs_.end();) {
        auto groupExpr = *iter;
        DCHECK(groupExpr != nullptr);
        if (groupExpr->isExplored(rule)) {
            ++iter;
            continue;
        }
        // Bottom to up exploration
        NG_RETURN_IF_ERROR(groupExpr->explore(rule));

        // Find more equivalents
        auto status = rule->match(groupExpr);
        if (!status.ok()) {
            ++iter;
            continue;
        }
        auto matched = std::move(status).value();
        auto resStatus = rule->transform(qctx_, matched);
        NG_RETURN_IF_ERROR(resStatus);
        auto result = std::move(resStatus).value();
        if (result.eraseAll) {
            groupExprs_.clear();
            for (auto nge : result.newGroupExprs) {
                addGroupExpr(nge);
            }
            break;
        }

        if (!result.newGroupExprs.empty()) {
            for (auto nge : result.newGroupExprs) {
                addGroupExpr(nge);
            }

            setUnexplored(rule);
        }

        if (result.eraseCurr) {
            iter = groupExprs_.erase(iter);
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

std::pair<double, const OptGroupExpr *> OptGroup::findMinCostGroupExpr() const {
    double minCost = std::numeric_limits<double>::max();
    const OptGroupExpr *minGroupExpr = nullptr;
    for (auto &groupExpr : groupExprs_) {
        double cost = groupExpr->getCost();
        if (minCost > cost) {
            minCost = cost;
            minGroupExpr = groupExpr;
        }
    }
    return std::make_pair(minCost, minGroupExpr);
}

double OptGroup::getCost() const {
    return findMinCostGroupExpr().first;
}

const PlanNode *OptGroup::getPlan() const {
    const OptGroupExpr *minGroupExpr = findMinCostGroupExpr().second;
    DCHECK(minGroupExpr != nullptr);
    return minGroupExpr->getPlan();
}

OptGroupExpr *OptGroupExpr::create(QueryContext *qctx, PlanNode *node, const OptGroup *group) {
    return qctx->objPool()->add(new OptGroupExpr(node, group));
}

OptGroupExpr::OptGroupExpr(PlanNode *node, const OptGroup *group) noexcept
    : node_(node), group_(group) {
    DCHECK(node != nullptr);
    DCHECK(group != nullptr);
}

Status OptGroupExpr::explore(const OptRule *rule) {
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

double OptGroupExpr::getCost() const {
    return node_->cost();
}

const PlanNode *OptGroupExpr::getPlan() const {
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
