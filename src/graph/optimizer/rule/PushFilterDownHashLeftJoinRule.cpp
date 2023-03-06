/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownHashLeftJoinRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownHashLeftJoinRule::kInstance =
    std::unique_ptr<PushFilterDownHashLeftJoinRule>(new PushFilterDownHashLeftJoinRule());

PushFilterDownHashLeftJoinRule::PushFilterDownHashLeftJoinRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownHashLeftJoinRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kFilter,
      {Pattern::create(
          PlanNode::Kind::kHashLeftJoin,
          {Pattern::create(PlanNode::Kind::kUnknown), Pattern::create(PlanNode::Kind::kUnknown)})});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownHashLeftJoinRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* filterGroupNode = matched.node;
  auto* oldFilterNode = filterGroupNode->node();
  DCHECK_EQ(oldFilterNode->kind(), PlanNode::Kind::kFilter);

  auto* leftJoinNode = matched.planNode({0, 0});
  DCHECK_EQ(leftJoinNode->kind(), PlanNode::Kind::kHashLeftJoin);
  auto* oldLeftJoinNode = static_cast<const graph::HashLeftJoin*>(leftJoinNode);

  const auto* condition = static_cast<graph::Filter*>(oldFilterNode)->condition();
  DCHECK(condition);

  const auto& leftResult = matched.result({0, 0, 0});
  const auto& rightResult = matched.result({0, 0, 1});
  OptGroup* rightGroup = const_cast<OptGroup*>(rightResult.node->group());

  Expression* leftFilterUnpicked = nullptr;
  OptGroup* leftGroup = pushFilterDownChild(octx, leftResult, condition, &leftFilterUnpicked);

  if (!leftGroup) {
    return TransformResult::noTransform();
  }

  // produce new LeftJoin node
  auto* newLeftJoinNode = static_cast<graph::HashLeftJoin*>(oldLeftJoinNode->clone());
  auto newJoinGroup = leftFilterUnpicked ? OptGroup::create(octx) : filterGroupNode->group();
  // TODO(yee): it's too tricky
  auto newGroupNode = leftFilterUnpicked
                          ? const_cast<OptGroup*>(newJoinGroup)->makeGroupNode(newLeftJoinNode)
                          : OptGroupNode::create(octx, newLeftJoinNode, newJoinGroup);
  newGroupNode->dependsOn(leftGroup);
  newGroupNode->dependsOn(rightGroup);
  newLeftJoinNode->setLeftVar(leftGroup->outputVar());
  newLeftJoinNode->setRightVar(rightGroup->outputVar());

  if (leftFilterUnpicked) {
    auto newFilterNode = graph::Filter::make(octx->qctx(), nullptr, leftFilterUnpicked);
    newFilterNode->setOutputVar(oldFilterNode->outputVar());
    newFilterNode->setColNames(oldFilterNode->colNames());
    newFilterNode->setInputVar(newLeftJoinNode->outputVar());
    newGroupNode = OptGroupNode::create(octx, newFilterNode, filterGroupNode->group());
    newGroupNode->dependsOn(const_cast<OptGroup*>(newJoinGroup));
  } else {
    newLeftJoinNode->setOutputVar(oldFilterNode->outputVar());
    newLeftJoinNode->setColNames(oldLeftJoinNode->colNames());
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newGroupNode);
  return result;
}

OptGroup* PushFilterDownHashLeftJoinRule::pushFilterDownChild(OptContext* octx,
                                                              const MatchedResult& child,
                                                              const Expression* condition,
                                                              Expression** unpickedFilter) {
  if (!condition) return nullptr;

  const auto* childPlanNode = DCHECK_NOTNULL(child.node->node());
  const auto& colNames = childPlanNode->colNames();

  // split the `condition` based on whether the varPropExpr comes from the left child
  auto picker = [&colNames](const Expression* e) -> bool {
    return graph::ExpressionUtils::checkColName(colNames, e);
  };

  Expression* filterPicked = nullptr;
  graph::ExpressionUtils::splitFilter(condition, picker, &filterPicked, unpickedFilter);
  if (!filterPicked) return nullptr;

  auto* newChildPlanNode = childPlanNode->clone();
  DCHECK_NE(childPlanNode->outputVar(), newChildPlanNode->outputVar());
  newChildPlanNode->setInputVar(childPlanNode->inputVar());
  newChildPlanNode->setColNames(childPlanNode->colNames());
  auto* newChildGroup = OptGroup::create(octx);
  auto* newChildGroupNode = newChildGroup->makeGroupNode(newChildPlanNode);
  for (auto* g : child.node->dependencies()) {
    newChildGroupNode->dependsOn(g);
  }

  auto* newFilterNode = graph::Filter::make(octx->qctx(), nullptr, filterPicked);
  newFilterNode->setOutputVar(childPlanNode->outputVar());
  newFilterNode->setColNames(colNames);
  newFilterNode->setInputVar(newChildPlanNode->outputVar());
  auto* group = OptGroup::create(octx);
  group->makeGroupNode(newFilterNode)->dependsOn(newChildGroup);
  return group;
}

std::string PushFilterDownHashLeftJoinRule::toString() const {
  return "PushFilterDownHashLeftJoinRule";
}

}  // namespace opt
}  // namespace nebula
