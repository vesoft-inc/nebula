/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/optimizer/rule/PushLimitDownTagIndexRangeScanRule.h"

#include "common/expression/BinaryExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/UnaryExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Scan.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::TagIndexRangeScan;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownTagIndexRangeScanRule::kInstance =
    std::unique_ptr<PushLimitDownTagIndexRangeScanRule>(new PushLimitDownTagIndexRangeScanRule());

PushLimitDownTagIndexRangeScanRule::PushLimitDownTagIndexRangeScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownTagIndexRangeScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kTagIndexRangeScan)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownTagIndexRangeScanRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto indexScanGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto indexScan = static_cast<const TagIndexRangeScan *>(indexScanGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count();
  if (indexScan->limit() >= 0 && limitRows >= indexScan->limit()) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newTagIndexRangeScan = static_cast<TagIndexRangeScan *>(indexScan->clone());
  newTagIndexRangeScan->setLimit(limitRows);
  auto newTagIndexRangeScanGroup = OptGroup::create(octx);
  auto newTagIndexRangeScanGroupNode =
      newTagIndexRangeScanGroup->makeGroupNode(newTagIndexRangeScan);

  newLimitGroupNode->dependsOn(newTagIndexRangeScanGroup);
  for (auto dep : indexScanGroupNode->dependencies()) {
    newTagIndexRangeScanGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownTagIndexRangeScanRule::toString() const {
  return "PushLimitDownTagIndexRangeScanRule";
}

}  // namespace opt
}  // namespace nebula
