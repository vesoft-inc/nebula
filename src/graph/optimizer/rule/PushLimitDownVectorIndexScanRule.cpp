/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownVectorIndexScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Explore;
using nebula::graph::VectorIndexScan;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownVectorIndexScanRule::kInstance =
    std::unique_ptr<PushLimitDownVectorIndexScanRule>(new PushLimitDownVectorIndexScanRule());

PushLimitDownVectorIndexScanRule::PushLimitDownVectorIndexScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushLimitDownVectorIndexScanRule::pattern() const {
  static Pattern pattern =
      Pattern::create({graph::PlanNode::Kind::kGetVertices, graph::PlanNode::Kind::kGetEdges},
                     {Pattern::create(graph::PlanNode::Kind::kVectorIndexScan)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownVectorIndexScanRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* qctx = octx->qctx();
  auto exploreGroupNode = matched.node;
  auto vectorGroupNode = matched.dependencies.front().node;

  const auto explore = static_cast<const Explore*>(exploreGroupNode->node());
  const auto vector = static_cast<const VectorIndexScan*>(vectorGroupNode->node());

  if (!graph::ExpressionUtils::isEvaluableExpr(explore->limitExpr())) {
    return TransformResult::noTransform();
  }
  int64_t limitRows = explore->limit(qctx);
  auto vectorLimit = vector->limit(qctx);
  if (vectorLimit >= 0 && limitRows >= vectorLimit) {
    return TransformResult::noTransform();
  }

  auto newExplore = static_cast<Explore*>(explore->clone());
  newExplore->setOutputVar(explore->outputVar());
  auto newExploreGroupNode = OptGroupNode::create(octx, newExplore, exploreGroupNode->group());

  auto newVector = static_cast<VectorIndexScan*>(vector->clone());
  newVector->setLimit(limitRows);
  auto newVectorGroup = OptGroup::create(octx);
  auto newVectorGroupNode = newVectorGroup->makeGroupNode(newVector);

  newExploreGroupNode->dependsOn(newVectorGroup);
  newExplore->setInputVar(newVector->outputVar());
  for (auto dep : vectorGroupNode->dependencies()) {
    newVectorGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newExploreGroupNode);
  return result;
}

std::string PushLimitDownVectorIndexScanRule::toString() const {
  return "PushLimitDownVectorIndexScanRule";
}

}  // namespace opt
}  // namespace nebula 