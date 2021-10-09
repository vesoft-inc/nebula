/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/optimizer/rule/PushLimitDownIndexScanRule.h"

#include "common/expression/BinaryExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/UnaryExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::graph::IndexScan;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownIndexScanRule::kInstance =
    std::unique_ptr<PushLimitDownIndexScanRule>(new PushLimitDownIndexScanRule());

PushLimitDownIndexScanRule::PushLimitDownIndexScanRule() { RuleSet::QueryRules().addRule(this); }

const Pattern &PushLimitDownIndexScanRule::pattern() const {
  static Pattern pattern =
      Pattern::create(graph::PlanNode::Kind::kLimit,
                      {Pattern::create(graph::PlanNode::Kind::kProject,
                                       {Pattern::create(graph::PlanNode::Kind::kIndexScan)})});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownIndexScanRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto projGroupNode = matched.dependencies.front().node;
  auto indexScanGroupNode = matched.dependencies.front().dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto proj = static_cast<const Project *>(projGroupNode->node());
  const auto indexScan = static_cast<const IndexScan *>(indexScanGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count();
  if (indexScan->limit() >= 0 && limitRows >= indexScan->limit()) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newProj = static_cast<Project *>(proj->clone());
  auto newProjGroup = OptGroup::create(octx);
  auto newProjGroupNode = newProjGroup->makeGroupNode(newProj);

  auto newIndexScan = static_cast<IndexScan *>(indexScan->clone());
  newIndexScan->setLimit(limitRows);
  auto newIndexScanGroup = OptGroup::create(octx);
  auto newIndexScanGroupNode = newIndexScanGroup->makeGroupNode(newIndexScan);

  newLimitGroupNode->dependsOn(newProjGroup);
  newProjGroupNode->dependsOn(newIndexScanGroup);
  for (auto dep : indexScanGroupNode->dependencies()) {
    newIndexScanGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownIndexScanRule::toString() const { return "PushLimitDownIndexScanRule"; }

}  // namespace opt
}  // namespace nebula
