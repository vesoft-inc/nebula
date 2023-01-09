/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushFilterDownTraverseRule.h"

#include "common/expression/ConstantExpression.h"
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

bool PushFilterDownTraverseRule::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  DCHECK_EQ(matched.dependencies[0].dependencies[0].node->node()->kind(),
            PlanNode::Kind::kTraverse);
  auto traverse =
      static_cast<const Traverse*>(matched.dependencies[0].dependencies[0].node->node());
  return traverse->isOneStep();
}

StatusOr<OptRule::TransformResult> PushFilterDownTraverseRule::transform(
    OptContext* ctx, const MatchedResult& matched) const {
  auto* filterGroupNode = matched.node;
  auto* filterGroup = filterGroupNode->group();
  auto* filter = static_cast<graph::Filter*>(filterGroupNode->node());
  auto* condition = filter->condition();

  auto* avGroupNode = matched.dependencies[0].node;
  auto* av = static_cast<graph::AppendVertices*>(avGroupNode->node());

  auto* tvGroupNode = matched.dependencies[0].dependencies[0].node;
  auto* tv = static_cast<graph::Traverse*>(tvGroupNode->node());
  auto& edgeAlias = tv->edgeAlias();

  auto qctx = ctx->qctx();
  auto pool = qctx->objPool();

  // Pick the expr looks like `$-.e[0].likeness
  auto picker = [&edgeAlias](const Expression* expr) -> bool {
    bool shouldNotPick = false;
    auto finder = [&shouldNotPick, &edgeAlias](const Expression* e) -> bool {
      // When visiting the expression tree and find an expession node is a one step edge property
      // expression, stop visiting its children and return true.
      if (graph::ExpressionUtils::isOneStepEdgeProp(edgeAlias, e)) return true;
      // Otherwise, continue visiting its children. And if the following two conditions are met,
      // mark the expression as shouldNotPick and return false.
      if (e->kind() == Expression::Kind::kInputProperty ||
          e->kind() == Expression::Kind::kVarProperty) {
        shouldNotPick = true;
        return false;
      }
      // TODO(jie): Handle the strange exists expr. e.g. exists(e.likeness)
      if (e->kind() == Expression::Kind::kPredicate &&
          static_cast<const PredicateExpression*>(e)->name() == "exists") {
        shouldNotPick = true;
        return false;
      }
      return false;
    };
    graph::FindVisitor visitor(finder, true, true);
    const_cast<Expression*>(expr)->accept(&visitor);
    if (shouldNotPick) return false;
    if (!visitor.results().empty()) {
      return true;
    }
    return false;
  };
  Expression* filterPicked = nullptr;
  Expression* filterUnpicked = nullptr;
  graph::ExpressionUtils::splitFilter(condition, picker, &filterPicked, &filterUnpicked);

  if (!filterPicked) {
    return TransformResult::noTransform();
  }
  auto* newFilterPicked =
      graph::ExpressionUtils::rewriteEdgePropertyFilter(pool, edgeAlias, filterPicked->clone());

  Filter* newFilter = nullptr;
  OptGroupNode* newFilterGroupNode = nullptr;
  if (filterUnpicked) {
    newFilter = Filter::make(qctx, nullptr, filterUnpicked);
    newFilter->setOutputVar(filter->outputVar());
    newFilter->setColNames(filter->colNames());
    newFilterGroupNode = OptGroupNode::create(ctx, newFilter, filterGroup);
  }

  auto* newAv = static_cast<graph::AppendVertices*>(av->clone());

  OptGroupNode* newAvGroupNode = nullptr;
  if (newFilterGroupNode) {
    auto* newAvGroup = OptGroup::create(ctx);
    newAvGroupNode = newAvGroup->makeGroupNode(newAv);
    newFilterGroupNode->dependsOn(newAvGroup);
    newFilter->setInputVar(newAv->outputVar());
  } else {
    newAvGroupNode = OptGroupNode::create(ctx, newAv, filterGroup);
    newAv->setOutputVar(filter->outputVar());
  }

  auto* eFilter = tv->eFilter();
  Expression* newEFilter = eFilter
                               ? LogicalExpression::makeAnd(pool, newFilterPicked, eFilter->clone())
                               : newFilterPicked;

  auto* newTv = static_cast<graph::Traverse*>(tv->clone());
  newAv->setInputVar(newTv->outputVar());
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
