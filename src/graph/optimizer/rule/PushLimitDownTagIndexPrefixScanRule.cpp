/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownTagIndexPrefixScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Scan.h"

using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::TagIndexPrefixScan;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownTagIndexPrefixScanRule::kInstance =
    std::unique_ptr<PushLimitDownTagIndexPrefixScanRule>(new PushLimitDownTagIndexPrefixScanRule());

PushLimitDownTagIndexPrefixScanRule::PushLimitDownTagIndexPrefixScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownTagIndexPrefixScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kTagIndexPrefixScan)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownTagIndexPrefixScanRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto limitGroupNode = matched.node;
  auto indexScanGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto indexScan = static_cast<const TagIndexPrefixScan *>(indexScanGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count(qctx);
  if (indexScan->limit(qctx) >= 0 && limitRows >= indexScan->limit(qctx)) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newTagIndexPrefixScan = static_cast<TagIndexPrefixScan *>(indexScan->clone());
  newTagIndexPrefixScan->setLimit(limitRows);
  auto newTagIndexPrefixScanGroup = OptGroup::create(octx);
  auto newTagIndexPrefixScanGroupNode =
      newTagIndexPrefixScanGroup->makeGroupNode(newTagIndexPrefixScan);

  newLimitGroupNode->dependsOn(newTagIndexPrefixScanGroup);
  for (auto dep : indexScanGroupNode->dependencies()) {
    newTagIndexPrefixScanGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownTagIndexPrefixScanRule::toString() const {
  return "PushLimitDownTagIndexPrefixScanRule";
}

}  // namespace opt
}  // namespace nebula
