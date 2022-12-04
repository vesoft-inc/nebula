/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/OptGroup.h"

#include <limits>

#include "graph/context/QueryContext.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"

using nebula::graph::BinaryInputNode;
using nebula::graph::Loop;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Select;
using nebula::graph::SingleDependencyNode;

namespace nebula {
namespace opt {

OptGroup *OptGroup::create(OptContext *ctx) {
  return ctx->objPool()->makeAndAdd<OptGroup>(ctx);
}

void OptGroup::setUnexplored(const OptRule *rule) {
  auto iter = std::find(exploredRules_.begin(), exploredRules_.end(), rule);
  if (iter != exploredRules_.end()) {
    exploredRules_.erase(iter);
  }
  for (auto node : groupNodes_) {
    node->setUnexplored(rule);
  }
}

OptGroup::OptGroup(OptContext *ctx) noexcept : ctx_(ctx) {
  DCHECK(ctx != nullptr);
}

void OptGroup::addGroupNode(OptGroupNode *groupNode) {
  DCHECK_EQ(this, DCHECK_NOTNULL(groupNode)->group());
  if (outputVar_.empty()) {
    outputVar_ = groupNode->node()->outputVar();
  } else {
    DCHECK_EQ(outputVar_, groupNode->node()->outputVar());
  }
  groupNodes_.emplace_back(groupNode);
  groupNode->node()->updateSymbols();
}

OptGroupNode *OptGroup::makeGroupNode(PlanNode *node) {
  if (outputVar_.empty()) {
    outputVar_ = node->outputVar();
  } else {
    DCHECK_EQ(outputVar_, node->outputVar());
  }
  groupNodes_.emplace_back(OptGroupNode::create(ctx_, node, this));
  return groupNodes_.back();
}

Status OptGroup::explore(const OptRule *rule) {
  if (isExplored(rule)) {
    return Status::OK();
  }
  setExplored(rule);

  DCHECK(!groupNodesReferenced_.empty())
      << "Current group should be referenced by other group nodes before optimization";

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
    std::vector<OptGroup *> boundary;
    auto status = rule->match(ctx_, groupNode);
    if (!status.ok()) {
      ++iter;
      continue;
    }
    ctx_->setChanged(true);
    auto matched = std::move(status).value();
    matched.collectBoundary(boundary);
    auto resStatus = rule->transform(ctx_, matched);
    NG_RETURN_IF_ERROR(resStatus);
    auto result = std::move(resStatus).value();
    // In some cases, we can apply optimization rules even if the control flow and data flow are
    // inconsistent. For now, let the optimization rules themselves guarantee correctness.
    if (result.eraseAll) {
      for (auto gnode : groupNodes_) {
        gnode->release();
      }
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
      (*iter)->release();
      iter = groupNodes_.erase(iter);
    } else {
      ++iter;
    }
  }

  DCHECK(!groupNodes_.empty())
      << "Should have at least one group node after optimizing the current group";

  return Status::OK();
}

Status OptGroup::exploreUntilMaxRound(const OptRule *rule) {
  auto maxRound = kMaxExplorationRound;
  while (!isExplored(rule)) {
    if (0 >= maxRound--) {
      setExplored(rule);
      break;
    }
    NG_RETURN_IF_ERROR(explore(rule));
  }
  return Status::OK();
}

std::pair<double, const OptGroupNode *> OptGroup::findMinCostGroupNode() const {
  DCHECK(!groupNodes_.empty()) << "There is no any group nodes in opt group";
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
  return DCHECK_NOTNULL(minGroupNode)->getPlan();
}

void OptGroup::deleteRefGroupNode(const OptGroupNode *node) {
  groupNodesReferenced_.erase(node);
  if (groupNodesReferenced_.empty()) {
    // Cleanup all opt group nodes in current opt group if it's NOT referenced by any other opt
    // group nodes
    for (auto *n : groupNodes_) {
      n->release();
    }
    groupNodes_.clear();
  }
}

OptGroupNode *OptGroupNode::create(OptContext *ctx, PlanNode *node, const OptGroup *group) {
  auto optGNode = ctx->objPool()->makeAndAdd<OptGroupNode>(node, group);
  ctx->addPlanNodeAndOptGroupNode(node->id(), optGNode);
  return optGNode;
}

void OptGroupNode::setUnexplored(const OptRule *rule) {
  auto iter = std::find(exploredRules_.begin(), exploredRules_.end(), rule);
  if (iter != exploredRules_.end()) {
    exploredRules_.erase(iter);
  }
  for (auto dep : dependencies_) {
    dep->setUnexplored(rule);
  }
  for (auto body : bodies_) {
    body->setUnexplored(rule);
  }
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
  DCHECK_EQ(node_->numDeps(), dependencies_.size());
  for (size_t i = 0; i < node_->numDeps(); ++i) {
    node_->setDep(i, dependencies_[i]->getPlan());
  }
  return node_;
}

void OptGroupNode::release() {
  node_->releaseSymbols();
  for (auto *dep : dependencies_) {
    dep->deleteRefGroupNode(this);
  }
}

}  // namespace opt
}  // namespace nebula
