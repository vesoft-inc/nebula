/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownHashInnerJoinRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownHashInnerJoinRule::kInstance =
    std::unique_ptr<PushFilterDownHashInnerJoinRule>(new PushFilterDownHashInnerJoinRule());

PushFilterDownHashInnerJoinRule::PushFilterDownHashInnerJoinRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownHashInnerJoinRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kFilter,
      {Pattern::create(
          PlanNode::Kind::kHashInnerJoin,
          {Pattern::create(PlanNode::Kind::kUnknown), Pattern::create(PlanNode::Kind::kUnknown)})});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownHashInnerJoinRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* filterGroupNode = matched.node;
  auto* oldFilterNode = filterGroupNode->node();
  DCHECK_EQ(oldFilterNode->kind(), PlanNode::Kind::kFilter);

  auto* innerJoinNode = matched.planNode({0, 0});
  DCHECK_EQ(innerJoinNode->kind(), PlanNode::Kind::kHashInnerJoin);
  auto* oldInnerJoinNode = static_cast<const graph::HashInnerJoin*>(innerJoinNode);

  const auto* condition = static_cast<graph::Filter*>(oldFilterNode)->condition();
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

  // produce new InnerJoin node
  auto* newInnerJoinNode = static_cast<graph::HashInnerJoin*>(oldInnerJoinNode->clone());
  auto newJoinGroup = rightFilterUnpicked ? OptGroup::create(octx) : filterGroupNode->group();
  // TODO(yee): it's too tricky
  auto newGroupNode = rightFilterUnpicked
                          ? const_cast<OptGroup*>(newJoinGroup)->makeGroupNode(newInnerJoinNode)
                          : OptGroupNode::create(octx, newInnerJoinNode, newJoinGroup);
  newGroupNode->dependsOn(leftGroup);
  newGroupNode->dependsOn(rightGroup);
  newInnerJoinNode->setLeftVar(leftGroup->outputVar());
  newInnerJoinNode->setRightVar(rightGroup->outputVar());

  if (rightFilterUnpicked) {
    auto newFilterNode = graph::Filter::make(octx->qctx(), nullptr, rightFilterUnpicked);
    newFilterNode->setOutputVar(oldFilterNode->outputVar());
    newFilterNode->setColNames(oldFilterNode->colNames());
    newFilterNode->setInputVar(newInnerJoinNode->outputVar());
    newGroupNode = OptGroupNode::create(octx, newFilterNode, filterGroupNode->group());
    newGroupNode->dependsOn(const_cast<OptGroup*>(newJoinGroup));
  } else {
    newInnerJoinNode->setOutputVar(oldFilterNode->outputVar());
    newInnerJoinNode->setColNames(oldInnerJoinNode->colNames());
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newGroupNode);
  return result;
}

OptGroup* PushFilterDownHashInnerJoinRule::pushFilterDownChild(OptContext* octx,
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

std::string PushFilterDownHashInnerJoinRule::toString() const {
  return "PushFilterDownHashInnerJoinRule";
}

}  // namespace opt
}  // namespace nebula
