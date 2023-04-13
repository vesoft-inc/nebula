/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/EmbedEdgeAllPredIntoTraverseRule.h"

#include "common/expression/AttributeExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/PredicateExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/VariableExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/RewriteVisitor.h"

using nebula::Expression;
using nebula::graph::Filter;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Traverse;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> EmbedEdgeAllPredIntoTraverseRule::kInstance =
    std::unique_ptr<EmbedEdgeAllPredIntoTraverseRule>(new EmbedEdgeAllPredIntoTraverseRule());

EmbedEdgeAllPredIntoTraverseRule::EmbedEdgeAllPredIntoTraverseRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& EmbedEdgeAllPredIntoTraverseRule::pattern() const {
  static Pattern pattern =
      Pattern::create(PlanNode::Kind::kFilter, {Pattern::create(PlanNode::Kind::kTraverse)});
  return pattern;
}

bool EmbedEdgeAllPredIntoTraverseRule::match(OptContext* ctx, const MatchedResult& matched) const {
  return OptRule::match(ctx, matched);
}

bool isEdgeAllPredicate(const Expression* e,
                        const std::string& edgeAlias,
                        std::string& innerEdgeVar) {
  // reset the inner edge var name
  innerEdgeVar = "";
  if (e->kind() != Expression::Kind::kPredicate) {
    return false;
  }
  auto* pe = static_cast<const PredicateExpression*>(e);
  if (pe->name() != "all" || !pe->hasInnerVar()) {
    return false;
  }
  auto var = pe->innerVar();
  if (!pe->collection()->isPropertyExpr()) {
    return false;
  }
  // check edge collection expression
  if (static_cast<const PropertyExpression*>(pe->collection())->prop() != edgeAlias) {
    return false;
  }
  auto ves = graph::ExpressionUtils::collectAll(pe->filter(), {Expression::Kind::kAttribute});
  if (ves.empty()) {
    return false;
  }
  for (const auto& ve : ves) {
    auto iv = static_cast<const AttributeExpression*>(ve)->left();

    // check inner vars
    if (iv->kind() != Expression::Kind::kVar) {
      return false;
    }
    // only care inner edge vars
    if (!static_cast<const VariableExpression*>(iv)->isInner()) {
      // FIXME(czp): support parameter/variables in edge `all` predicate
      return false;
    }

    // edge property in AttributeExpression must be Constant string
    auto ep = static_cast<const AttributeExpression*>(ve)->right();
    if (ep->kind() != Expression::Kind::kConstant) {
      return false;
    }
    if (!static_cast<const ConstantExpression*>(ep)->value().isStr()) {
      return false;
    }
  }

  innerEdgeVar = var;
  return true;
}

// Pick sub-predicate
// rewrite edge `all` predicates to single-hop edge predicate
Expression* rewriteEdgeAllPredicate(const Expression* expr, const std::string& edgeAlias) {
  std::string innerEdgeVar;
  auto matcher = [&edgeAlias, &innerEdgeVar](const Expression* e) -> bool {
    return isEdgeAllPredicate(e, edgeAlias, innerEdgeVar);
  };
  auto rewriter = [&innerEdgeVar](const Expression* e) -> Expression* {
    DCHECK_EQ(e->kind(), Expression::Kind::kPredicate);
    auto fe = static_cast<const PredicateExpression*>(e)->filter();

    auto innerMatcher = [&innerEdgeVar](const Expression* ae) {
      if (ae->kind() != Expression::Kind::kAttribute) {
        return false;
      }
      auto innerEdgeVarExpr = static_cast<const AttributeExpression*>(ae)->left();
      if (innerEdgeVarExpr->kind() != Expression::Kind::kVar) {
        return false;
      }
      return static_cast<const VariableExpression*>(innerEdgeVarExpr)->var() == innerEdgeVar;
    };

    auto innerRewriter = [](const Expression* ae) {
      DCHECK_EQ(ae->kind(), Expression::Kind::kAttribute);
      auto attributeExpr = static_cast<const AttributeExpression*>(ae);
      auto* right = attributeExpr->right();
      // edge property name expressions have been checked in the external matcher
      DCHECK_EQ(right->kind(), Expression::Kind::kConstant);
      auto& prop = static_cast<const ConstantExpression*>(right)->value().getStr();
      return EdgePropertyExpression::make(ae->getObjPool(), "*", prop);
    };
    // Rewrite all the inner var edge attribute expressions of `all` predicate's oldFilterNode to
    // EdgePropertyExpression
    return graph::RewriteVisitor::transform(fe, std::move(innerMatcher), std::move(innerRewriter));
  };
  return graph::RewriteVisitor::transform(expr, std::move(matcher), std::move(rewriter));
}

StatusOr<OptRule::TransformResult> EmbedEdgeAllPredIntoTraverseRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* oldFilterGroupNode = matched.node;
  auto* oldFilterGroup = oldFilterGroupNode->group();
  auto* oldFilterNode = static_cast<graph::Filter*>(oldFilterGroupNode->node());
  auto* condition = oldFilterNode->condition();
  auto* oldTvGroupNode = matched.dependencies[0].node;
  auto* oldTvNode = static_cast<graph::Traverse*>(oldTvGroupNode->node());
  auto& edgeAlias = oldTvNode->edgeAlias();
  auto qctx = octx->qctx();

  // Pick all predicates containing edge `all` predicates under the AND semantics
  auto picker = [&edgeAlias](const Expression* expr) -> bool {
    bool neverPicked = false;
    auto finder = [&neverPicked, &edgeAlias](const Expression* e) -> bool {
      if (neverPicked) {
        return false;
      }
      // UnaryNot change the semantics of `all` predicate to `any`, resulting in the inability to
      // scatter the edge `all` predicate into a single-hop edge predicate(not cover double-not
      // cases)
      if (e->kind() == Expression::Kind::kUnaryNot) {
        neverPicked = true;
        return false;
      }
      // Not used, the picker only cares if there is an edge `all` predicate in the current operand
      std::string innerVar;
      return isEdgeAllPredicate(e, edgeAlias, innerVar);
    };
    graph::FindVisitor visitor(finder);
    const_cast<Expression*>(expr)->accept(&visitor);
    return !visitor.results().empty();
  };
  Expression* filterPicked = nullptr;
  Expression* filterUnpicked = nullptr;
  graph::ExpressionUtils::splitFilter(condition, picker, &filterPicked, &filterUnpicked);

  if (!filterPicked) {
    return TransformResult::noTransform();
  }

  // reconnect the existing edge filters
  auto* edgeFilter = rewriteEdgeAllPredicate(filterPicked, edgeAlias);
  auto* oldEdgeFilter = oldTvNode->eFilter();
  Expression* newEdgeFilter =
      oldEdgeFilter ? LogicalExpression::makeAnd(
                          oldEdgeFilter->getObjPool(), edgeFilter, oldEdgeFilter->clone())
                    : edgeFilter;

  // produce new Traverse node
  auto* newTvNode = static_cast<graph::Traverse*>(oldTvNode->clone());
  newTvNode->setEdgeFilter(newEdgeFilter);
  newTvNode->setInputVar(oldTvNode->inputVar());
  newTvNode->setColNames(oldTvNode->outputVarPtr()->colNames);

  // connect the optimized plan
  TransformResult result;
  result.eraseAll = true;
  if (filterUnpicked) {
    // assemble the new Filter node with the old Filter group
    auto* newAboveFilterNode = graph::Filter::make(qctx, newTvNode, filterUnpicked);
    newAboveFilterNode->setOutputVar(oldFilterNode->outputVar());
    newAboveFilterNode->setColNames(oldFilterNode->colNames());
    auto newAboveFilterGroupNode = OptGroupNode::create(octx, newAboveFilterNode, oldFilterGroup);
    // assemble the new Traverse group below Filter
    auto newTvGroup = OptGroup::create(octx);
    auto newTvGroupNode = newTvGroup->makeGroupNode(newTvNode);
    newTvGroupNode->setDeps(oldTvGroupNode->dependencies());
    newAboveFilterGroupNode->setDeps({newTvGroup});
    newAboveFilterNode->setInputVar(newTvNode->outputVar());
    result.newGroupNodes.emplace_back(newAboveFilterGroupNode);
  } else {
    // replace the new Traverse node with the old Filter group
    auto newTvGroupNode = OptGroupNode::create(octx, newTvNode, oldFilterGroup);
    newTvNode->setOutputVar(oldFilterNode->outputVar());
    newTvGroupNode->setDeps(oldTvGroupNode->dependencies());
    result.newGroupNodes.emplace_back(newTvGroupNode);
  }

  return result;
}

std::string EmbedEdgeAllPredIntoTraverseRule::toString() const {
  return "EmbedEdgeAllPredIntoTraverseRule";
}

}  // namespace opt
}  // namespace nebula
