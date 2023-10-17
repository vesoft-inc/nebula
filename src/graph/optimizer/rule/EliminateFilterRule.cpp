/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/EliminateFilterRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::Filter;
using nebula::graph::QueryContext;
using nebula::graph::StartNode;
using nebula::graph::ValueNode;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> EliminateFilterRule::kInstance =
    std::unique_ptr<EliminateFilterRule>(new EliminateFilterRule());

EliminateFilterRule::EliminateFilterRule() {
  RuleSet::QueryRules0().addRule(this);
}

// TODO match Filter->(Any Node with real result)
const Pattern& EliminateFilterRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kFilter);
  return pattern;
}

bool EliminateFilterRule::match(OptContext* octx, const MatchedResult& matched) const {
  if (!OptRule::match(octx, matched)) {
    return false;
  }
  const auto* filterNode = static_cast<const Filter*>(matched.node->node());
  const auto* expr = filterNode->condition();
  if (expr->kind() != Expression::Kind::kConstant) {
    return false;
  }
  const auto* constant = static_cast<const ConstantExpression*>(expr);
  auto ret = (constant->value().isImplicitBool() && constant->value().getBool() == false) ||
             constant->value().isNull();
  return ret;
}

StatusOr<OptRule::TransformResult> EliminateFilterRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto filterGroupNode = matched.node;
  auto filter = static_cast<const Filter*>(filterGroupNode->node());

  auto newStart = StartNode::make(octx->qctx());
  auto newStartGroup = OptGroup::create(octx);
  newStartGroup->makeGroupNode(newStart);

  auto newValue = ValueNode::make(octx->qctx(), newStart, DataSet(filter->colNames()));
  newValue->setOutputVar(filter->outputVar());
  auto newValueGroupNode = OptGroupNode::create(octx, newValue, filterGroupNode->group());
  newValueGroupNode->dependsOn(newStartGroup);

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newValueGroupNode);
  return result;
}

std::string EliminateFilterRule::toString() const {
  return "EliminateFilterRule";
}

}  // namespace opt
}  // namespace nebula
