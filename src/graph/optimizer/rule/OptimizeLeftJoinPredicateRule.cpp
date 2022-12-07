// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/optimizer/rule/OptimizeLeftJoinPredicateRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> OptimizeLeftJoinPredicateRule::kInstance =
    std::unique_ptr<OptimizeLeftJoinPredicateRule>(new OptimizeLeftJoinPredicateRule());

OptimizeLeftJoinPredicateRule::OptimizeLeftJoinPredicateRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& OptimizeLeftJoinPredicateRule::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kHashLeftJoin,
      {Pattern::create(PlanNode::Kind::kUnknown),
       Pattern::create(PlanNode::Kind::kProject,
                       {Pattern::create(PlanNode::Kind::kAppendVertices,
                                        {Pattern::create(PlanNode::Kind::kTraverse)})})});
  return pattern;
}

StatusOr<OptRule::TransformResult> OptimizeLeftJoinPredicateRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* leftJoinGroupNode = matched.node;
  auto* leftJoinGroup = leftJoinGroupNode->group();
  auto* leftJoin = static_cast<graph::HashLeftJoin*>(leftJoinGroupNode->node());

  auto* projectGroupNode = matched.dependencies[1].node;
  auto* project = static_cast<graph::Project*>(projectGroupNode->node());

  auto* appendVerticesGroupNode = matched.dependencies[1].dependencies[0].node;
  auto appendVertices =
      static_cast<graph::AppendVertices*>(matched.dependencies[1].dependencies[0].node->node());

  auto traverse = static_cast<graph::Traverse*>(
      matched.dependencies[1].dependencies[0].dependencies[0].node->node());

  auto& avNodeAlias = appendVertices->nodeAlias();

  auto& tvEdgeAlias = traverse->edgeAlias();

  auto& leftExprs = leftJoin->hashKeys();
  auto& rightExprs = leftJoin->probeKeys();

  bool found = false;
  size_t rightExprIdx = 0;
  for (size_t i = 0; i < rightExprs.size(); ++i) {
    auto* rightExpr = rightExprs[i];
    if (rightExpr->kind() != Expression::Kind::kFunctionCall) {
      continue;
    }
    auto* func = static_cast<FunctionCallExpression*>(rightExpr);
    if (func->name() != "id" && func->name() != "_joinkey") {
      continue;
    }
    auto& args = func->args()->args();
    DCHECK_EQ(args.size(), 1);
    auto* arg = args[0];
    if (arg->kind() != Expression::Kind::kInputProperty) {
      continue;
    }
    auto& alias = static_cast<InputPropertyExpression*>(arg)->prop();
    if (alias != avNodeAlias) continue;
    // Must check if left exprs contain the same key
    if (*leftExprs[i] != *rightExpr) {
      return TransformResult::noTransform();
    }
    if (found) {
      return TransformResult::noTransform();
    }
    rightExprIdx = i;
    found = true;
  }
  if (!found) {
    return TransformResult::noTransform();
  }

  found = false;
  size_t prjIdx = 0;
  auto* columns = project->columns();
  for (size_t i = 0; i < columns->size(); ++i) {
    const auto* col = columns->columns()[i];
    if (col->expr()->kind() != Expression::Kind::kInputProperty) {
      continue;
    }
    auto* inputProp = static_cast<InputPropertyExpression*>(col->expr());
    if (inputProp->prop() != avNodeAlias) continue;
    if (found) {
      return TransformResult::noTransform();
    }
    prjIdx = i;
    found = true;
  }
  if (!found) {
    return TransformResult::noTransform();
  }

  auto* pool = octx->qctx()->objPool();
  // Let the new project generate expr `none_direct_dst($-.tvEdgeAlias)`,
  // and let the new left join use it as right expr
  auto* args = ArgumentList::make(pool);
  args->addArgument(InputPropertyExpression::make(pool, tvEdgeAlias));
  auto* newPrjExpr = FunctionCallExpression::make(pool, "none_direct_dst", args);

  auto oldYieldColumns = project->columns()->columns();
  auto* newYieldColumns = pool->makeAndAdd<YieldColumns>();
  for (size_t i = 0; i < oldYieldColumns.size(); ++i) {
    if (i == prjIdx) {
      newYieldColumns->addColumn(new YieldColumn(newPrjExpr, avNodeAlias));
    } else {
      newYieldColumns->addColumn(oldYieldColumns[i]->clone().release());
    }
  }
  auto* newProject = graph::Project::make(octx->qctx(), nullptr, newYieldColumns);

  // $-.`avNodeAlias`
  auto* newRightExpr = InputPropertyExpression::make(pool, avNodeAlias);
  std::vector<Expression*> newRightExprs;
  for (size_t i = 0; i < rightExprs.size(); ++i) {
    if (i == rightExprIdx) {
      newRightExprs.emplace_back(newRightExpr);
    } else {
      newRightExprs.emplace_back(rightExprs[i]->clone());
    }
  }
  auto* newLeftJoin =
      graph::HashLeftJoin::make(octx->qctx(), nullptr, nullptr, leftExprs, newRightExprs);

  TransformResult result;
  result.eraseAll = true;

  newProject->setInputVar(appendVertices->inputVar());
  auto newProjectGroup = OptGroup::create(octx);
  auto* newProjectGroupNode = newProjectGroup->makeGroupNode(newProject);
  newProjectGroupNode->setDeps(appendVerticesGroupNode->dependencies());

  newLeftJoin->setLeftVar(leftJoin->leftInputVar());
  newLeftJoin->setRightVar(newProject->outputVar());
  newLeftJoin->setOutputVar(leftJoin->outputVar());
  auto* newLeftJoinGroupNode = OptGroupNode::create(octx, newLeftJoin, leftJoinGroup);
  newLeftJoinGroupNode->dependsOn(leftJoinGroupNode->dependencies()[0]);
  newLeftJoinGroupNode->dependsOn(newProjectGroup);

  result.newGroupNodes.emplace_back(newLeftJoinGroupNode);
  return result;
}

std::string OptimizeLeftJoinPredicateRule::toString() const {
  return "OptimizeLeftJoinPredicateRule";
}

}  // namespace opt
}  // namespace nebula
