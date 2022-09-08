/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownInnerJoinRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownInnerJoinRule::kInstance =
    std::unique_ptr<PushFilterDownInnerJoinRule>(new PushFilterDownInnerJoinRule());

PushFilterDownInnerJoinRule::PushFilterDownInnerJoinRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownInnerJoinRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter,
                                           {Pattern::create(graph::PlanNode::Kind::kInnerJoin)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownInnerJoinRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* filterGroupNode = matched.node;
  auto* oldFilterNode = filterGroupNode->node();
  auto deps = matched.dependencies;
  DCHECK_EQ(deps.size(), 1);
  auto innerJoinGroupNode = deps.front().node;
  auto* innerJoinNode = innerJoinGroupNode->node();
  DCHECK_EQ(oldFilterNode->kind(), PlanNode::Kind::kFilter);
  DCHECK_EQ(innerJoinNode->kind(), PlanNode::Kind::kInnerJoin);
  auto* oldInnerJoinNode = static_cast<graph::InnerJoin*>(innerJoinNode);
  const auto* condition = static_cast<graph::Filter*>(oldFilterNode)->condition();
  DCHECK(condition);
  const std::pair<std::string, int64_t>& leftVar = oldInnerJoinNode->leftVar();
  auto symTable = octx->qctx()->symTable();
  std::vector<std::string> leftVarColNames = symTable->getVar(leftVar.first)->colNames;

  // split the `condition` based on whether the varPropExpr comes from the left
  // child
  auto picker = [&leftVarColNames](const Expression* e) -> bool {
    auto varProps = graph::ExpressionUtils::collectAll(e, {Expression::Kind::kVarProperty});
    if (varProps.empty()) {
      return false;
    }
    std::vector<std::string> propNames;
    for (auto* expr : varProps) {
      DCHECK(expr->kind() == Expression::Kind::kVarProperty);
      propNames.emplace_back(static_cast<const VariablePropertyExpression*>(expr)->prop());
    }
    for (auto prop : propNames) {
      auto iter = std::find_if(leftVarColNames.begin(),
                               leftVarColNames.end(),
                               [&prop](std::string item) { return !item.compare(prop); });
      if (iter == leftVarColNames.end()) {
        return false;
      }
    }
    return true;
  };
  Expression* filterPicked = nullptr;
  Expression* filterUnpicked = nullptr;
  graph::ExpressionUtils::splitFilter(condition, picker, &filterPicked, &filterUnpicked);

  if (!filterPicked) {
    return TransformResult::noTransform();
  }

  // produce new left Filter node
  auto* newLeftFilterNode =
      graph::Filter::make(octx->qctx(),
                          const_cast<graph::PlanNode*>(oldInnerJoinNode->dep()),
                          graph::ExpressionUtils::rewriteInnerVar(filterPicked, leftVar.first));
  newLeftFilterNode->setInputVar(leftVar.first);
  newLeftFilterNode->setColNames(leftVarColNames);
  auto newFilterGroup = OptGroup::create(octx);
  auto newFilterGroupNode = newFilterGroup->makeGroupNode(newLeftFilterNode);
  for (auto dep : innerJoinGroupNode->dependencies()) {
    newFilterGroupNode->dependsOn(dep);
  }
  auto newLeftFilterOutputVar = newLeftFilterNode->outputVar();

  // produce new InnerJoin node
  auto* newInnerJoinNode = static_cast<graph::InnerJoin*>(oldInnerJoinNode->clone());
  newInnerJoinNode->setLeftVar({newLeftFilterOutputVar, 0});
  const std::vector<Expression*>& hashKeys = oldInnerJoinNode->hashKeys();
  std::vector<Expression*> newHashKeys;
  for (auto* k : hashKeys) {
    newHashKeys.emplace_back(graph::ExpressionUtils::rewriteInnerVar(k, newLeftFilterOutputVar));
  }
  newInnerJoinNode->setHashKeys(newHashKeys);

  TransformResult result;
  result.eraseAll = true;
  if (filterUnpicked) {
    auto* newAboveFilterNode = graph::Filter::make(octx->qctx(), newInnerJoinNode);
    newAboveFilterNode->setOutputVar(oldFilterNode->outputVar());
    newAboveFilterNode->setCondition(filterUnpicked);
    auto newAboveFilterGroupNode =
        OptGroupNode::create(octx, newAboveFilterNode, filterGroupNode->group());

    auto newInnerJoinGroup = OptGroup::create(octx);
    auto newInnerJoinGroupNode = newInnerJoinGroup->makeGroupNode(newInnerJoinNode);
    newAboveFilterGroupNode->setDeps({newInnerJoinGroup});
    newInnerJoinGroupNode->setDeps({newFilterGroup});
    result.newGroupNodes.emplace_back(newAboveFilterGroupNode);
  } else {
    newInnerJoinNode->setOutputVar(oldFilterNode->outputVar());
    newInnerJoinNode->setColNames(oldInnerJoinNode->colNames());
    auto newInnerJoinGroupNode =
        OptGroupNode::create(octx, newInnerJoinNode, filterGroupNode->group());
    newInnerJoinGroupNode->setDeps({newFilterGroup});
    result.newGroupNodes.emplace_back(newInnerJoinGroupNode);
  }
  return result;
}

std::string PushFilterDownInnerJoinRule::toString() const {
  return "PushFilterDownInnerJoinRule";
}

}  // namespace opt
}  // namespace nebula
