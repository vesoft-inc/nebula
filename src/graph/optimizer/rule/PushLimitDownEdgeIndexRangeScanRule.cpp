/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownEdgeIndexRangeScanRule.h"

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

using nebula::graph::EdgeIndexRangeScan;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownEdgeIndexRangeScanRule::kInstance =
    std::unique_ptr<PushLimitDownEdgeIndexRangeScanRule>(new PushLimitDownEdgeIndexRangeScanRule());

PushLimitDownEdgeIndexRangeScanRule::PushLimitDownEdgeIndexRangeScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownEdgeIndexRangeScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit, {Pattern::create(graph::PlanNode::Kind::kEdgeIndexRangeScan)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownEdgeIndexRangeScanRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto indexScanGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto indexScan = static_cast<const EdgeIndexRangeScan *>(indexScanGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count();
  if (indexScan->limit() >= 0 && limitRows >= indexScan->limit()) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newEdgeIndexRangeScan = static_cast<EdgeIndexRangeScan *>(indexScan->clone());
  newEdgeIndexRangeScan->setLimit(limitRows);
  auto newEdgeIndexRangeScanGroup = OptGroup::create(octx);
  auto newEdgeIndexRangeScanGroupNode =
      newEdgeIndexRangeScanGroup->makeGroupNode(newEdgeIndexRangeScan);

  newLimitGroupNode->dependsOn(newEdgeIndexRangeScanGroup);
  for (auto dep : indexScanGroupNode->dependencies()) {
    newEdgeIndexRangeScanGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownEdgeIndexRangeScanRule::toString() const {
  return "PushLimitDownEdgeIndexRangeScanRule";
}

}  // namespace opt
}  // namespace nebula
