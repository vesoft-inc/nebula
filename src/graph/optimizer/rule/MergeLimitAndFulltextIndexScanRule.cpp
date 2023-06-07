/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/MergeLimitAndFulltextIndexScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Explore;
using nebula::graph::FulltextIndexScan;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> MergeLimitAndFulltextIndexScanRule::kInstance =
    std::unique_ptr<MergeLimitAndFulltextIndexScanRule>(new MergeLimitAndFulltextIndexScanRule());

MergeLimitAndFulltextIndexScanRule::MergeLimitAndFulltextIndexScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &MergeLimitAndFulltextIndexScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kLimit,
      {Pattern::create({graph::PlanNode::Kind::kGetVertices, graph::PlanNode::Kind::kGetEdges},
                       {Pattern::create(graph::PlanNode::Kind::kFulltextIndexScan)})});
  return pattern;
}

bool MergeLimitAndFulltextIndexScanRule::match(OptContext *, const MatchedResult &matched) const {
  auto limit = static_cast<const Limit *>(matched.planNode());
  if (limit->offset() <= 0) {
    return false;
  }
  auto explore = static_cast<const Explore *>(matched.planNode({0, 0}));
  if (explore->limit() <= 0) {
    return false;
  }
  auto ft = static_cast<const FulltextIndexScan *>(matched.planNode({0, 0, 0}));
  return ft->limit() >= 0 && ft->offset() < 0;
}

StatusOr<OptRule::TransformResult> MergeLimitAndFulltextIndexScanRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.result().node;
  auto exploreGroupNode = matched.result({0, 0}).node;
  auto ftGroupNode = matched.result({0, 0, 0}).node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto explore = static_cast<const Explore *>(exploreGroupNode->node());
  const auto ft = static_cast<const FulltextIndexScan *>(ftGroupNode->node());

  auto limitRows = limit->count() + limit->offset();
  if (limitRows != explore->limit() || limitRows != ft->limit()) {
    return TransformResult::noTransform();
  }

  auto newExplore = static_cast<Explore *>(explore->clone());
  newExplore->setOutputVar(limit->outputVar());
  auto newExploreGroupNode = OptGroupNode::create(octx, newExplore, limitGroupNode->group());

  auto newFt = static_cast<FulltextIndexScan *>(ft->clone());
  newFt->setOffset(limit->offset());
  auto newFtGroup = OptGroup::create(octx);
  auto newFtGroupNode = newFtGroup->makeGroupNode(newFt);

  newExploreGroupNode->dependsOn(newFtGroup);
  newExplore->setInputVar(newFt->outputVar());
  for (auto dep : ftGroupNode->dependencies()) {
    newFtGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newExploreGroupNode);
  return result;
}

std::string MergeLimitAndFulltextIndexScanRule::toString() const {
  return "MergeLimitAndFulltextIndexScanRule";
}

}  // namespace opt
}  // namespace nebula
