/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownTraverseRule.h"

#include "common/expression/Expression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::Expression;
using nebula::graph::Filter;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Traverse;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushFilterDownTraverseRule::kInstance =
    std::unique_ptr<PushFilterDownTraverseRule>(new PushFilterDownTraverseRule());

PushFilterDownTraverseRule::PushFilterDownTraverseRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushFilterDownTraverseRule::pattern() const {
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kFilter,
                      {Pattern::create(PlanNode::Kind::kAppendVertices,
                                       {Pattern::create(PlanNode::Kind::kTraverse)})});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushFilterDownTraverseRule::transform(
    OptContext* ctx, const MatchedResult& matched) const {
  auto* filterGroupNode = matched.node;
  auto* filterGroup = filterGroupNode->group();
  auto* filter = static_cast<graph::Filter*>(filterGroupNode->node());
  auto* condition = filter->condition();

  auto* avGroupNode = matched.dependencies[0].node;
  auto* av = static_cast<graph::AppendVertices*>(avGroupNode->node());
  auto& avColNames = av->colNames();
  auto& nodeAlias = avColNames.back();
  UNUSED(nodeAlias);

  auto* tvGroupNode = matched.dependencies[0].dependencies[0].node;
  auto* tv = static_cast<graph::Traverse*>(tvGroupNode->node());
  auto& tvColNames = tv->colNames();
  auto& edgeAlias = tvColNames.back();

  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();

  auto picker = [&edgeAlias](const Expression* e) -> bool {
    auto varProps = graph::ExpressionUtils::collectAll(e,
                                                       {Expression::Kind::kTagProperty,
                                                        Expression::Kind::kEdgeProperty,
                                                        Expression::Kind::kInputProperty,
                                                        Expression::Kind::kVarProperty,
                                                        Expression::Kind::kDstProperty,
                                                        Expression::Kind::kSrcProperty});
    if (varProps.empty()) {
      return false;
    }
    for (auto* expr : varProps) {
      DCHECK(graph::ExpressionUtils::isPropertyExpr(expr));
      auto& propName = static_cast<const PropertyExpression*>(expr)->prop();
      if (propName != edgeAlias) return false;
    }
    return true;
  };
  Expression* filterPicked = nullptr;
  Expression* filterUnpicked = nullptr;
  graph::ExpressionUtils::splitFilter(condition, picker, &filterPicked, &filterUnpicked);

  if (!filterPicked) {
    return TransformResult::noTransform();
  }

  Filter* newFilter = nullptr;
  OptGroupNode* newFilterGroupNode = nullptr;
  if (filterUnpicked) {
    newFilter = Filter::make(qctx, nullptr, filterUnpicked);
    // newFilter->setOutputVar(filter->outputVar());
    // newFilter->setInputVar(filter->inputVar());
    newFilterGroupNode = OptGroupNode::create(ctx, newFilter, filterGroup);
  }

  auto* newAv = static_cast<graph::AppendVertices*>(av->clone());
  // newFilter->setInputVar(newAv->outputVar());

  OptGroupNode* newAvGroupNode = nullptr;
  if (newFilterGroupNode) {
    auto* newAvGroup = OptGroup::create(ctx);
    newAvGroupNode = newAvGroup->makeGroupNode(newAv);
    newFilterGroupNode->dependsOn(newAvGroup);
  } else {
    newAvGroupNode = OptGroupNode::create(ctx, newAv, filterGroup);
  }

  auto* eFilter = tv->eFilter();
  Expression* newEFilter = nullptr;
  if (eFilter) {
    auto logicExpr = LogicalExpression::makeAnd(pool, filterPicked, eFilter->clone());
    newEFilter = logicExpr;
  } else {
    newEFilter = filterPicked;
  }

  auto* newTv = static_cast<graph::Traverse*>(tv->clone());
  newTv->setEdgeFilter(newEFilter);

  auto* newTvGroup = OptGroup::create(ctx);
  newAvGroupNode->dependsOn(newTvGroup);
  auto* newTvGroupNode = newTvGroup->makeGroupNode(newTv);

  for (auto dep : tvGroupNode->dependencies()) {
    newTvGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseCurr = true;
  result.newGroupNodes.emplace_back(newFilterGroupNode ? newFilterGroupNode : newAvGroupNode);

  return result;
}

std::string PushFilterDownTraverseRule::toString() const {
  return "PushFilterDownTraverseRule";
}

}  // namespace opt
}  // namespace nebula
