/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/optimizer/rule/PushStepSampleDownGetNeighborsRule.h"

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
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Sample;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushStepSampleDownGetNeighborsRule::kInstance =
    std::unique_ptr<PushStepSampleDownGetNeighborsRule>(new PushStepSampleDownGetNeighborsRule());

PushStepSampleDownGetNeighborsRule::PushStepSampleDownGetNeighborsRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushStepSampleDownGetNeighborsRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kSample,
                                           {Pattern::create(graph::PlanNode::Kind::kGetNeighbors)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushStepSampleDownGetNeighborsRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto sampleGroupNode = matched.node;
  auto gnGroupNode = matched.dependencies.front().node;

  const auto sample = static_cast<const Sample *>(sampleGroupNode->node());
  const auto gn = static_cast<const GetNeighbors *>(gnGroupNode->node());

  if (gn->limitExpr() != nullptr && graph::ExpressionUtils::isEvaluableExpr(gn->limitExpr()) &&
      graph::ExpressionUtils::isEvaluableExpr(sample->countExpr())) {
    int64_t limitRows = sample->count();
    int64_t gnLimit = gn->limit();
    if (gnLimit >= 0 && limitRows >= gnLimit) {
      return TransformResult::noTransform();
    }
  }

  auto newSample = static_cast<Sample *>(sample->clone());
  auto newSampleGroupNode = OptGroupNode::create(octx, newSample, sampleGroupNode->group());

  auto newGn = static_cast<GetNeighbors *>(gn->clone());
  newGn->setLimit(sample->countExpr()->clone());
  newGn->setRandom(true);
  auto newGnGroup = OptGroup::create(octx);
  auto newGnGroupNode = newGnGroup->makeGroupNode(newGn);

  newSampleGroupNode->dependsOn(newGnGroup);
  for (auto dep : gnGroupNode->dependencies()) {
    newGnGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newSampleGroupNode);
  return result;
}

std::string PushStepSampleDownGetNeighborsRule::toString() const {
  return "PushStepSampleDownGetNeighborsRule";
}

}  // namespace opt
}  // namespace nebula
