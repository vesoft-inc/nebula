/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownEdgeIndexPrefixScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Scan.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::graph::EdgeIndexPrefixScan;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownEdgeIndexPrefixScanRule::kInstance =
    std::unique_ptr<PushLimitDownEdgeIndexPrefixScanRule>(
        new PushLimitDownEdgeIndexPrefixScanRule());

PushLimitDownEdgeIndexPrefixScanRule::PushLimitDownEdgeIndexPrefixScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushLimitDownEdgeIndexPrefixScanRule::pattern() const {
  static Pattern pattern =
      Pattern::create(graph::PlanNode::Kind::kLimit,
                      {Pattern::create(graph::PlanNode::Kind::kEdgeIndexPrefixScan)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownEdgeIndexPrefixScanRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* qctx = octx->qctx();
  auto limitGroupNode = matched.node;
  auto indexScanGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit*>(limitGroupNode->node());
  const auto indexScan = static_cast<const EdgeIndexPrefixScan*>(indexScanGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count(qctx);
  if (indexScan->limit(qctx) >= 0 && limitRows >= indexScan->limit(qctx)) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit*>(limit->clone());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newEdgeIndexPrefixScan = static_cast<EdgeIndexPrefixScan*>(indexScan->clone());
  newEdgeIndexPrefixScan->setLimit(limitRows);
  auto newEdgeIndexPrefixScanGroup = OptGroup::create(octx);
  auto newEdgeIndexPrefixScanGroupNode =
      newEdgeIndexPrefixScanGroup->makeGroupNode(newEdgeIndexPrefixScan);

  newLimitGroupNode->dependsOn(newEdgeIndexPrefixScanGroup);
  for (auto dep : indexScanGroupNode->dependencies()) {
    newEdgeIndexPrefixScanGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownEdgeIndexPrefixScanRule::toString() const {
  return "PushLimitDownEdgeIndexPrefixScanRule";
}

}  // namespace opt
}  // namespace nebula
