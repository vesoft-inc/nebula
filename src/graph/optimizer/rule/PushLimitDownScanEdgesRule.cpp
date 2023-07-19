// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/optimizer/rule/PushLimitDownScanEdgesRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::ScanEdges;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownScanEdgesRule::kInstance =
    std::unique_ptr<PushLimitDownScanEdgesRule>(new PushLimitDownScanEdgesRule());

PushLimitDownScanEdgesRule::PushLimitDownScanEdgesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownScanEdgesRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kScanEdges)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownScanEdgesRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto limitGroupNode = matched.node;
  auto seGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto se = static_cast<const ScanEdges *>(seGroupNode->node());

  if (!graph::ExpressionUtils::isEvaluableExpr(limit->countExpr())) {
    return TransformResult::noTransform();
  }
  int64_t limitRows = limit->offset() + limit->count(qctx);
  if (se->limit(qctx) >= 0 && limitRows >= se->limit(qctx)) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  newLimit->setOutputVar(limit->outputVar());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newSe = static_cast<ScanEdges *>(se->clone());
  newSe->setLimit(limitRows);
  auto newSeGroup = OptGroup::create(octx);
  auto newSeGroupNode = newSeGroup->makeGroupNode(newSe);

  newLimitGroupNode->dependsOn(newSeGroup);
  newLimit->setInputVar(newSe->outputVar());
  for (auto dep : seGroupNode->dependencies()) {
    newSeGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownScanEdgesRule::toString() const {
  return "PushLimitDownScanEdgesRule";
}

}  // namespace opt
}  // namespace nebula
