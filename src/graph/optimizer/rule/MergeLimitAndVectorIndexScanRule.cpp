/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/MergeLimitAndVectorIndexScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Explore;
using nebula::graph::VectorIndexScan;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> MergeLimitAndVectorIndexScanRule::kInstance =
    std::unique_ptr<MergeLimitAndVectorIndexScanRule>(new MergeLimitAndVectorIndexScanRule());

MergeLimitAndVectorIndexScanRule::MergeLimitAndVectorIndexScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& MergeLimitAndVectorIndexScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kLimit,
      {Pattern::create({graph::PlanNode::Kind::kGetVertices, graph::PlanNode::Kind::kGetEdges},
                      {Pattern::create(graph::PlanNode::Kind::kVectorIndexScan)})});
  return pattern;
}

bool MergeLimitAndVectorIndexScanRule::match(OptContext*, const MatchedResult& matched) const {
  auto limit = static_cast<const Limit*>(matched.planNode());
  if (limit->offset() <= 0) {
    return false;
  }
  auto explore = static_cast<const Explore*>(matched.planNode({0, 0}));
  if (explore->limit() <= 0) {
    return false;
  }
  auto vector = static_cast<const VectorIndexScan*>(matched.planNode({0, 0, 0}));
  return vector->limit() >= 0 && vector->offset() < 0;
}

StatusOr<OptRule::TransformResult> MergeLimitAndVectorIndexScanRule::transform(
    OptContext* octx,
    const MatchedResult& matched) const {
  auto limitGroupNode = matched.result().node;
  auto exploreGroupNode = matched.result({0, 0}).node;
  auto vectorGroupNode = matched.result({0, 0, 0}).node;

  const auto limit = static_cast<const Limit*>(limitGroupNode->node());
  const auto explore = static_cast<const Explore*>(exploreGroupNode->node());
  const auto vector = static_cast<const VectorIndexScan*>(vectorGroupNode->node());

  auto limitRows = limit->count() + limit->offset();
  if (limitRows != explore->limit() || limitRows != vector->limit()) {
    return TransformResult::noTransform();
  }

  auto newExplore = static_cast<Explore*>(explore->clone());
  newExplore->setOutputVar(limit->outputVar());
  auto newExploreGroupNode = OptGroupNode::create(octx, newExplore, limitGroupNode->group());

  auto newVector = static_cast<VectorIndexScan*>(vector->clone());
  newVector->setOffset(limit->offset());
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

std::string MergeLimitAndVectorIndexScanRule::toString() const {
  return "MergeLimitAndVectorIndexScanRule";
}

}  // namespace opt
}  // namespace nebula
