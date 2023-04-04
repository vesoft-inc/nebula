/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownCrossJoinRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::CrossJoin;
using nebula::graph::ExpressionUtils;
using nebula::graph::Filter;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownCrossJoinRule::kInstance =
    std::unique_ptr<PushFilterDownCrossJoinRule>(new PushFilterDownCrossJoinRule());

PushFilterDownCrossJoinRule::PushFilterDownCrossJoinRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownCrossJoinRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kFilter,
      {Pattern::create(
          PlanNode::Kind::kCrossJoin,
          {Pattern::create(PlanNode::Kind::kUnknown), Pattern::create(PlanNode::Kind::kUnknown)})});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownCrossJoinRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* filterGroupNode = matched.node;
  auto* oldFilterNode = filterGroupNode->node();
  DCHECK_EQ(oldFilterNode->kind(), PlanNode::Kind::kFilter);

  auto* crossJoinNode = matched.planNode({0, 0});
  DCHECK_EQ(crossJoinNode->kind(), PlanNode::Kind::kCrossJoin);
  auto* oldCrossJoinNode = static_cast<const CrossJoin*>(crossJoinNode);

  const auto* condition = static_cast<Filter*>(oldFilterNode)->condition();
  DCHECK(condition);

  const auto& leftResult = matched.result({0, 0, 0});
  const auto& rightResult = matched.result({0, 0, 1});

  Expression *leftFilterUnpicked = nullptr, *rightFilterUnpicked = nullptr;
  OptGroup* leftGroup = pushFilterDownChild(octx, leftResult, condition, &leftFilterUnpicked);
  OptGroup* rightGroup =
      pushFilterDownChild(octx, rightResult, leftFilterUnpicked, &rightFilterUnpicked);

  if (!leftGroup && !rightGroup) {
    return TransformResult::noTransform();
  }

  leftGroup = leftGroup ? leftGroup : const_cast<OptGroup*>(leftResult.node->group());
  rightGroup = rightGroup ? rightGroup : const_cast<OptGroup*>(rightResult.node->group());

  // produce new CrossJoin node
  auto* newCrossJoinNode = static_cast<CrossJoin*>(oldCrossJoinNode->clone());
  auto newJoinGroup = rightFilterUnpicked ? OptGroup::create(octx) : filterGroupNode->group();
  // TODO(yee): it's too tricky
  auto newGroupNode = rightFilterUnpicked
                          ? const_cast<OptGroup*>(newJoinGroup)->makeGroupNode(newCrossJoinNode)
                          : OptGroupNode::create(octx, newCrossJoinNode, newJoinGroup);
  newGroupNode->dependsOn(leftGroup);
  newGroupNode->dependsOn(rightGroup);
  newCrossJoinNode->setLeftVar(leftGroup->outputVar());
  newCrossJoinNode->setRightVar(rightGroup->outputVar());

  if (rightFilterUnpicked) {
    auto newFilterNode = Filter::make(octx->qctx(), nullptr, rightFilterUnpicked);
    newFilterNode->setOutputVar(oldFilterNode->outputVar());
    newFilterNode->setColNames(oldFilterNode->colNames());
    newFilterNode->setInputVar(newCrossJoinNode->outputVar());
    newGroupNode = OptGroupNode::create(octx, newFilterNode, filterGroupNode->group());
    newGroupNode->dependsOn(const_cast<OptGroup*>(newJoinGroup));
  } else {
    newCrossJoinNode->setOutputVar(oldFilterNode->outputVar());
    newCrossJoinNode->setColNames(oldCrossJoinNode->colNames());
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newGroupNode);
  return result;
}

OptGroup* PushFilterDownCrossJoinRule::pushFilterDownChild(OptContext* octx,
                                                           const MatchedResult& child,
                                                           const Expression* condition,
                                                           Expression** unpickedFilter) {
  if (!condition) return nullptr;

  const auto* childPlanNode = DCHECK_NOTNULL(child.node->node());
  const auto& colNames = childPlanNode->colNames();

  // split the `condition` based on whether the varPropExpr comes from the left child
  auto picker = [&colNames](const Expression* e) -> bool {
    return ExpressionUtils::checkColName(colNames, e);
  };

  Expression* filterPicked = nullptr;
  ExpressionUtils::splitFilter(condition, picker, &filterPicked, unpickedFilter);
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

  auto* newFilterNode = Filter::make(octx->qctx(), nullptr, filterPicked);
  newFilterNode->setOutputVar(childPlanNode->outputVar());
  newFilterNode->setColNames(colNames);
  newFilterNode->setInputVar(newChildPlanNode->outputVar());
  auto* group = OptGroup::create(octx);
  group->makeGroupNode(newFilterNode)->dependsOn(newChildGroup);
  return group;
}

std::string PushFilterDownCrossJoinRule::toString() const {
  return "PushFilterDownCrossJoinRule";
}

}  // namespace opt
}  // namespace nebula
