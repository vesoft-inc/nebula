/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/CollapseProjectRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

DEFINE_bool(enable_optimizer_collapse_project_rule, true, "");

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> CollapseProjectRule::kInstance =
    std::unique_ptr<CollapseProjectRule>(new CollapseProjectRule());

CollapseProjectRule::CollapseProjectRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& CollapseProjectRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kProject,
                                           {Pattern::create(graph::PlanNode::Kind::kProject)});
  return pattern;
}

StatusOr<OptRule::TransformResult> CollapseProjectRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  const auto* groupNodeAbove = matched.node;
  const auto* projAbove = groupNodeAbove->node();
  DCHECK_EQ(projAbove->kind(), PlanNode::Kind::kProject);
  const auto& deps = matched.dependencies;
  DCHECK_EQ(deps.size(), 1);
  const auto* groupNodeBelow = deps[0].node;
  const auto* projBelow = groupNodeBelow->node();
  DCHECK_EQ(projBelow->kind(), PlanNode::Kind::kProject);
  std::vector<YieldColumn*> colsBelow =
      static_cast<const graph::Project*>(projBelow)->columns()->columns();
  const auto* projGroup = groupNodeAbove->group();

  auto* newProj = static_cast<graph::Project*>(projAbove->clone());
  std::vector<YieldColumn*> colsAbove = newProj->columns()->columns();

  // 1. collect all property reference
  std::vector<std::string> allPropRefNames;
  for (auto col : colsAbove) {
    std::vector<const Expression*> propRefs = graph::ExpressionUtils::collectAll(
        col->expr(), {Expression::Kind::kVarProperty, Expression::Kind::kInputProperty});
    for (auto* expr : propRefs) {
      DCHECK(expr->kind() == Expression::Kind::kVarProperty ||
             expr->kind() == Expression::Kind::kInputProperty);
      allPropRefNames.emplace_back(static_cast<const PropertyExpression*>(expr)->prop());
    }
  }

  // disable this case to avoid the expression in ProjBelow being eval multiple
  // times
  std::unordered_set<std::string> uniquePropRefNames;
  std::unordered_set<std::string> multiRefColNames;
  for (auto p : allPropRefNames) {
    if (!uniquePropRefNames.insert(p).second) {
      // Records PropRefNames that are referenced multiple times
      multiRefColNames.insert(p);
    }
  }

  // 2. find link according to propRefNames and colNames in ProjBelow
  std::unordered_map<std::string, Expression*> rewriteMap;
  auto colNames = projBelow->colNames();
  DCHECK_EQ(colNames.size(), colsBelow.size());
  for (size_t i = 0; i < colNames.size(); ++i) {
    if (uniquePropRefNames.count(colNames[i])) {
      auto colExpr = colsBelow[i]->expr();
      // disable this case to avoid the expression in ProjBelow being eval multiple
      // times
      if (!graph::ExpressionUtils::isPropertyExpr(colExpr) && multiRefColNames.count(colNames[i])) {
        return TransformResult::noTransform();
      }
      rewriteMap[colNames[i]] = colExpr;
    }
  }

  // 3. rewrite YieldColumns
  auto matcher = [&rewriteMap](const Expression* e) -> bool {
    if (e->kind() != Expression::Kind::kVarProperty &&
        e->kind() != Expression::Kind::kInputProperty) {
      return false;
    }
    auto& propName = static_cast<const PropertyExpression*>(e)->prop();
    return rewriteMap.find(propName) != rewriteMap.end();
  };
  auto rewriter = [&rewriteMap](const Expression* e) -> Expression* {
    DCHECK(e->kind() == Expression::Kind::kVarProperty ||
           e->kind() == Expression::Kind::kInputProperty);
    auto& propName = static_cast<const PropertyExpression*>(e)->prop();
    return rewriteMap[propName]->clone();
  };
  for (auto col : colsAbove) {
    auto* newColExpr = graph::RewriteVisitor::transform(col->expr(), matcher, rewriter);
    col->setExpr(newColExpr);
  }

  // 4. rebuild OptGroupNode
  newProj->setInputVar(projBelow->inputVar());
  newProj->setOutputVar(projAbove->outputVar());
  auto* newGroupNode = OptGroupNode::create(octx, newProj, projGroup);
  newGroupNode->setDeps(groupNodeBelow->dependencies());

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newGroupNode);

  return result;
}

bool CollapseProjectRule::match(OptContext* octx, const MatchedResult& matched) const {
  if (!FLAGS_enable_optimizer_collapse_project_rule) {
    return false;
  }
  return OptRule::match(octx, matched);
}

std::string CollapseProjectRule::toString() const {
  return "CollapseProjectRule";
}

}  // namespace opt
}  // namespace nebula
