/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownAllPathsRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::AllPaths;
using nebula::graph::Filter;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownAllPathsRule::kInstance =
    std::unique_ptr<PushFilterDownAllPathsRule>(new PushFilterDownAllPathsRule());

PushFilterDownAllPathsRule::PushFilterDownAllPathsRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushFilterDownAllPathsRule::pattern() const {
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kFilter, {Pattern::create(PlanNode::Kind::kAllPaths)});
  return pattern;
}

bool PushFilterDownAllPathsRule::match(OptContext *ctx, const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto path = static_cast<const AllPaths *>(matched.planNode({0, 0}));
  auto edgeProps = path->edgeProps();
  // if fetching props of edge in AllPaths, let it go and do more checks in
  // transform. otherwise skip this rule.
  return edgeProps != nullptr && !edgeProps->empty();
}

StatusOr<OptRule::TransformResult> PushFilterDownAllPathsRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto filterGroupNode = matched.node;
  auto pathGroupNode = matched.dependencies.front().node;
  auto filter = static_cast<const Filter *>(filterGroupNode->node());
  auto path = static_cast<const AllPaths *>(pathGroupNode->node());
  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();
  auto condition = filter->condition()->clone();

  graph::ExtractFilterExprVisitor visitor(pool, qctx->spaceId(), qctx->schemaMng());
  condition->accept(&visitor);
  if (!visitor.ok()) {
    return TransformResult::noTransform();
  }

  auto remainedExpr = std::move(visitor).remainedExpr();
  if (remainedExpr != nullptr) {
    return TransformResult::noTransform();
  }

  auto newPathFilter = condition;
  if (path->filter() != nullptr) {
    auto logicExpr = LogicalExpression::makeAnd(pool, condition, path->filter()->clone());
    newPathFilter = logicExpr;
  }

  auto newPath = static_cast<AllPaths *>(path->clone());
  newPath->setFilter(newPathFilter);

  OptGroupNode *newPathGroupNode = nullptr;
  // Filter(A)<-AllPaths(C) => AllPaths(A&&C)
  newPathGroupNode = OptGroupNode::create(ctx, newPath, filterGroupNode->group());
  newPath->setOutputVar(filter->outputVar());

  for (auto dep : pathGroupNode->dependencies()) {
    newPathGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newPathGroupNode);
  return result;
}

std::string PushFilterDownAllPathsRule::toString() const {
  return "PushFilterDownAllPathsRule";
}

}  // namespace opt
}  // namespace nebula
