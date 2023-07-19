/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownGetEdgesRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::GetEdges;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownGetEdgesRule::kInstance =
    std::unique_ptr<PushLimitDownGetEdgesRule>(new PushLimitDownGetEdgesRule());

PushLimitDownGetEdgesRule::PushLimitDownGetEdgesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownGetEdgesRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kGetEdges)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownGetEdgesRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto limitGroupNode = matched.node;
  auto geGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto ge = static_cast<const GetEdges *>(geGroupNode->node());

  if (!graph::ExpressionUtils::isEvaluableExpr(limit->countExpr())) {
    return TransformResult::noTransform();
  }
  int64_t limitRows = limit->offset() + limit->count(qctx);
  auto geLimit = ge->limit(qctx);
  if (geLimit >= 0 && limitRows >= geLimit) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  newLimit->setOutputVar(limit->outputVar());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newGe = static_cast<GetEdges *>(ge->clone());
  newGe->setLimit(limitRows);
  auto newGeGroup = OptGroup::create(octx);
  auto newGeGroupNode = newGeGroup->makeGroupNode(newGe);

  newLimitGroupNode->dependsOn(newGeGroup);
  newLimit->setInputVar(newGe->outputVar());
  for (auto dep : geGroupNode->dependencies()) {
    newGeGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownGetEdgesRule::toString() const {
  return "PushLimitDownGetEdgesRule";
}

}  // namespace opt
}  // namespace nebula
