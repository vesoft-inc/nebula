/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownGetVerticesRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::GetVertices;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownGetVerticesRule::kInstance =
    std::unique_ptr<PushLimitDownGetVerticesRule>(new PushLimitDownGetVerticesRule());

PushLimitDownGetVerticesRule::PushLimitDownGetVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownGetVerticesRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kGetVertices)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownGetVerticesRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto limitGroupNode = matched.node;
  auto gvGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto gv = static_cast<const GetVertices *>(gvGroupNode->node());

  if (!graph::ExpressionUtils::isEvaluableExpr(limit->countExpr())) {
    return TransformResult::noTransform();
  }
  int64_t limitRows = limit->offset() + limit->count(qctx);
  auto gvLimit = gv->limit(qctx);
  if (gvLimit >= 0 && limitRows >= gvLimit) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  newLimit->setOutputVar(limit->outputVar());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newGv = static_cast<GetVertices *>(gv->clone());
  newGv->setLimit(limitRows);
  auto newGvGroup = OptGroup::create(octx);
  auto newGvGroupNode = newGvGroup->makeGroupNode(newGv);

  newLimitGroupNode->dependsOn(newGvGroup);
  newLimit->setInputVar(newGv->outputVar());
  for (auto dep : gvGroupNode->dependencies()) {
    newGvGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownGetVerticesRule::toString() const {
  return "PushLimitDownGetVerticesRule";
}

}  // namespace opt
}  // namespace nebula
