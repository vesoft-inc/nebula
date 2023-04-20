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
      Pattern::create(PlanNode::Kind::kFilter, {Pattern::create(PlanNode::Kind::kTraverse)});
  return pattern;
}

bool PushFilterDownTraverseRule::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  DCHECK_EQ(matched.dependencies[0].node->node()->kind(), PlanNode::Kind::kTraverse);
  auto traverse = static_cast<const Traverse*>(matched.dependencies[0].node->node());
  return traverse->isOneStep();
}

StatusOr<OptRule::TransformResult> PushFilterDownTraverseRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* filterGNode = matched.node;
  auto* filterGroup = filterGNode->group();
  auto* filter = static_cast<graph::Filter*>(filterGNode->node());
  auto* condition = filter->condition();

  auto* tvGNode = matched.dependencies[0].node;
  auto* tvNode = static_cast<graph::Traverse*>(tvGNode->node());
  auto& edgeAlias = tvNode->edgeAlias();

  auto qctx = octx->qctx();
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
  auto* eFilter = tvNode->eFilter();
  Expression* newEFilter = eFilter
                               ? LogicalExpression::makeAnd(pool, newFilterPicked, eFilter->clone())
                               : newFilterPicked;

  // produce new Traverse node
  auto* newTvNode = static_cast<graph::Traverse*>(tvNode->clone());
  newTvNode->setEdgeFilter(newEFilter);
  newTvNode->setInputVar(tvNode->inputVar());
  newTvNode->setColNames(tvNode->outputVarPtr()->colNames);

  // connect the optimized plan
  TransformResult result;
  result.eraseAll = true;
  if (filterUnpicked) {
    auto* newFilterNode = graph::Filter::make(qctx, newTvNode, filterUnpicked);
    newFilterNode->setOutputVar(filter->outputVar());
    newFilterNode->setColNames(filter->colNames());
    auto newFilterGNode = OptGroupNode::create(octx, newFilterNode, filterGroup);
    // assemble the new Traverse group below Filter
    auto newTvGroup = OptGroup::create(octx);
    auto newTvGNode = newTvGroup->makeGroupNode(newTvNode);
    newTvGNode->setDeps(tvGNode->dependencies());
    newFilterGNode->setDeps({newTvGroup});
    newFilterNode->setInputVar(newTvNode->outputVar());
    result.newGroupNodes.emplace_back(newFilterGNode);
  } else {
    // replace the new Traverse node with the old Filter group
    auto newTvGNode = OptGroupNode::create(octx, newTvNode, filterGroup);
    newTvNode->setOutputVar(filter->outputVar());
    newTvGNode->setDeps(tvGNode->dependencies());
    result.newGroupNodes.emplace_back(newTvGNode);
  }

  return result;
}

std::string PushFilterDownTraverseRule::toString() const {
  return "PushFilterDownTraverseRule";
}

}  // namespace opt
}  // namespace nebula
