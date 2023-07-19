/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushStepLimitDownGetNeighborsRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::GetNeighbors;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushStepLimitDownGetNeighborsRule::kInstance =
    std::unique_ptr<PushStepLimitDownGetNeighborsRule>(new PushStepLimitDownGetNeighborsRule());

PushStepLimitDownGetNeighborsRule::PushStepLimitDownGetNeighborsRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushStepLimitDownGetNeighborsRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kGetNeighbors)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushStepLimitDownGetNeighborsRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto gnGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto gn = static_cast<const GetNeighbors *>(gnGroupNode->node());
  auto *qctx = octx->qctx();
  if (gn->limitExpr() != nullptr &&
      graph::ExpressionUtils::isEvaluableExpr(gn->limitExpr(), qctx) &&
      graph::ExpressionUtils::isEvaluableExpr(limit->countExpr(), qctx)) {
    int64_t limitRows = limit->offset() + limit->count(qctx);
    int64_t gnLimit = gn->limit(qctx);
    if (gnLimit >= 0 && limitRows >= gnLimit) {
      return TransformResult::noTransform();
    }
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  newLimit->setOutputVar(limit->outputVar());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newGn = static_cast<GetNeighbors *>(gn->clone());
  newGn->setLimit(limit->countExpr()->clone());
  auto newGnGroup = OptGroup::create(octx);
  auto newGnGroupNode = newGnGroup->makeGroupNode(newGn);

  newLimitGroupNode->dependsOn(newGnGroup);
  newLimit->setInputVar(newGn->outputVar());
  for (auto dep : gnGroupNode->dependencies()) {
    newGnGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushStepLimitDownGetNeighborsRule::toString() const {
  return "PushStepLimitDownGetNeighborsRule";
}

}  // namespace opt
}  // namespace nebula
