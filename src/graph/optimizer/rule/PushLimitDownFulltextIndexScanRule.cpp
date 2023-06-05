/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownFulltextIndexScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Explore;
using nebula::graph::FulltextIndexScan;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownFulltextIndexScanRule::kInstance =
    std::unique_ptr<PushLimitDownFulltextIndexScanRule>(new PushLimitDownFulltextIndexScanRule());

PushLimitDownFulltextIndexScanRule::PushLimitDownFulltextIndexScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownFulltextIndexScanRule::pattern() const {
  static Pattern pattern =
      Pattern::create({graph::PlanNode::Kind::kGetNeighbors, graph::PlanNode::Kind::kGetEdges},
                      {Pattern::create(graph::PlanNode::Kind::kFulltextIndexScan)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownFulltextIndexScanRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto exploreGroupNode = matched.node;
  auto ftGroupNode = matched.dependencies.front().node;

  const auto explore = static_cast<const Explore *>(exploreGroupNode->node());
  const auto ft = static_cast<const FulltextIndexScan *>(ftGroupNode->node());

  if (!graph::ExpressionUtils::isEvaluableExpr(explore->limitExpr())) {
    return TransformResult::noTransform();
  }
  int64_t limitRows = explore->limit(qctx);
  auto ftLimit = ft->limit(qctx);
  if (ftLimit >= 0 && limitRows >= ftLimit) {
    return TransformResult::noTransform();
  }

  auto newExplore = static_cast<Explore *>(explore->clone());
  newExplore->setOutputVar(explore->outputVar());
  auto newExploreGroupNode = OptGroupNode::create(octx, newExplore, exploreGroupNode->group());

  auto newFt = static_cast<FulltextIndexScan *>(ft->clone());
  newFt->setLimit(limitRows);
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

std::string PushLimitDownFulltextIndexScanRule::toString() const {
  return "PushLimitDownFulltextIndexScanRule";
}

}  // namespace opt
}  // namespace nebula
