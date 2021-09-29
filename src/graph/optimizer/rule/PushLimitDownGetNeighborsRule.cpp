/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/optimizer/rule/PushLimitDownGetNeighborsRule.h"

#include "common/expression/BinaryExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/UnaryExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::graph::GetNeighbors;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownGetNeighborsRule::kInstance =
    std::unique_ptr<PushLimitDownGetNeighborsRule>(new PushLimitDownGetNeighborsRule());

PushLimitDownGetNeighborsRule::PushLimitDownGetNeighborsRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownGetNeighborsRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kGetNeighbors)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownGetNeighborsRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto gnGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto gn = static_cast<const GetNeighbors *>(gnGroupNode->node());

  if (!graph::ExpressionUtils::isEvaluableExpr(limit->countExpr())) {
    return TransformResult::noTransform();
  }
  int64_t limitRows = limit->offset() + limit->count();
  if (gn->limit() >= 0 && limitRows >= gn->limit()) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newGn = static_cast<GetNeighbors *>(gn->clone());
  newGn->setLimit(limitRows);
  auto newGnGroup = OptGroup::create(octx);
  auto newGnGroupNode = newGnGroup->makeGroupNode(newGn);

  newLimitGroupNode->dependsOn(newGnGroup);
  for (auto dep : gnGroupNode->dependencies()) {
    newGnGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownGetNeighborsRule::toString() const {
  return "PushLimitDownGetNeighborsRule";
}

}  // namespace opt
}  // namespace nebula
