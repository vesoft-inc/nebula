/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownExpandAllRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::ExpandAll;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownExpandAllRule::kInstance =
    std::unique_ptr<PushLimitDownExpandAllRule>(new PushLimitDownExpandAllRule());

PushLimitDownExpandAllRule::PushLimitDownExpandAllRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownExpandAllRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kExpandAll)});
  return pattern;
}

bool PushLimitDownExpandAllRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto expandAll = static_cast<const ExpandAll *>(matched.planNode({0, 0}));
  return expandAll->minSteps() == expandAll->maxSteps();
}

StatusOr<OptRule::TransformResult> PushLimitDownExpandAllRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto *qctx = octx->qctx();
  auto limitGroupNode = matched.node;
  auto expandAllGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto expandAll = static_cast<const ExpandAll *>(expandAllGroupNode->node());

  if (!graph::ExpressionUtils::isEvaluableExpr(limit->countExpr())) {
    return TransformResult::noTransform();
  }
  int64_t limitRows = limit->offset() + limit->count(qctx);
  if (expandAll->limit(qctx) >= 0 && limitRows >= expandAll->limit(qctx)) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  newLimit->setOutputVar(limit->outputVar());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newExpandAll = static_cast<ExpandAll *>(expandAll->clone());
  newExpandAll->setLimit(limitRows);
  auto newExpandAllGroup = OptGroup::create(octx);
  auto newExpandAllGroupNode = newExpandAllGroup->makeGroupNode(newExpandAll);

  newLimitGroupNode->dependsOn(newExpandAllGroup);
  newLimit->setInputVar(newExpandAll->outputVar());
  for (auto dep : expandAllGroupNode->dependencies()) {
    newExpandAllGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownExpandAllRule::toString() const {
  return "PushLimitDownExpandAllRule";
}

}  // namespace opt
}  // namespace nebula
