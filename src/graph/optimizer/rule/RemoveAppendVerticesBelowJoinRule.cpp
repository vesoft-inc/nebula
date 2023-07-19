// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/optimizer/rule/RemoveAppendVerticesBelowJoinRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> RemoveAppendVerticesBelowJoinRule::kInstance =
    std::unique_ptr<RemoveAppendVerticesBelowJoinRule>(new RemoveAppendVerticesBelowJoinRule());

RemoveAppendVerticesBelowJoinRule::RemoveAppendVerticesBelowJoinRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& RemoveAppendVerticesBelowJoinRule::pattern() const {
  static Pattern pattern = Pattern::create(
      {PlanNode::Kind::kHashLeftJoin, PlanNode::Kind::kHashInnerJoin},
      {Pattern::create(PlanNode::Kind::kUnknown),
       Pattern::create(PlanNode::Kind::kProject,
                       {Pattern::create(PlanNode::Kind::kAppendVertices,
                                        {Pattern::create(PlanNode::Kind::kTraverse)})})});
  return pattern;
}

StatusOr<OptRule::TransformResult> RemoveAppendVerticesBelowJoinRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* joinGroupNode = matched.node;
  auto* joinGroup = joinGroupNode->group();
  auto* join = static_cast<graph::HashJoin*>(joinGroupNode->node());

  auto* projectGroupNode = matched.dependencies[1].node;
  auto* project = static_cast<graph::Project*>(projectGroupNode->node());

  auto* appendVerticesGroupNode = matched.dependencies[1].dependencies[0].node;
  auto appendVertices =
      static_cast<graph::AppendVertices*>(matched.dependencies[1].dependencies[0].node->node());

  auto traverse = static_cast<graph::Traverse*>(
      matched.dependencies[1].dependencies[0].dependencies[0].node->node());

  auto& avNodeAlias = appendVertices->nodeAlias();

  auto& tvEdgeAlias = traverse->edgeAlias();
  auto& tvNodeAlias = traverse->nodeAlias();

  auto& leftExprs = join->hashKeys();
  auto& rightExprs = join->probeKeys();

  std::vector<const Expression*> referAVNodeAliasExprs;
  for (auto* rightExpr : rightExprs) {
    auto propExprs = graph::ExpressionUtils::collectAll(
        rightExpr, {Expression::Kind::kVarProperty, Expression::Kind::kInputProperty});
    for (auto* expr : propExprs) {
      auto* propExpr = static_cast<const PropertyExpression*>(expr);
      if (propExpr->prop() == avNodeAlias) {
        referAVNodeAliasExprs.push_back(propExpr);
      }
    }
  }
  // If avNodeAlias is referred by more than one expr,
  // we cannot remove the append vertices which generate the avNodeAlias column
  if (referAVNodeAliasExprs.size() > 1) {
    return TransformResult::noTransform();
  }

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

  auto columns = project->columns()->columns();
  referAVNodeAliasExprs.clear();
  for (auto* column : columns) {
    auto propExprs = graph::ExpressionUtils::collectAll(
        column->expr(), {Expression::Kind::kVarProperty, Expression::Kind::kInputProperty});
    for (auto* expr : propExprs) {
      auto* propExpr = static_cast<const PropertyExpression*>(expr);
      if (propExpr->prop() == avNodeAlias) {
        referAVNodeAliasExprs.push_back(propExpr);
      }
    }
  }
  // If avNodeAlias is referred by more than one expr,
  // we cannot remove the append vertices which generate the avNodeAlias column
  if (referAVNodeAliasExprs.size() > 1) {
    return TransformResult::noTransform();
  }

  found = false;
  size_t prjIdx = 0;
  for (size_t i = 0; i < columns.size(); ++i) {
    const auto* col = columns[i];
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
  // and let the new left/inner join use it as right expr
  auto* args = ArgumentList::make(pool);
  args->addArgument(InputPropertyExpression::make(pool, tvEdgeAlias));
  args->addArgument(InputPropertyExpression::make(pool, tvNodeAlias));
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
  graph::HashJoin* newJoin = nullptr;
  if (join->kind() == PlanNode::Kind::kHashLeftJoin) {
    newJoin = graph::HashLeftJoin::make(octx->qctx(), nullptr, nullptr, leftExprs, newRightExprs);
  } else {
    newJoin = graph::HashInnerJoin::make(octx->qctx(), nullptr, nullptr, leftExprs, newRightExprs);
  }

  TransformResult result;
  result.eraseAll = true;

  newProject->setInputVar(appendVertices->inputVar());
  auto newProjectGroup = OptGroup::create(octx);
  auto* newProjectGroupNode = newProjectGroup->makeGroupNode(newProject);
  newProjectGroupNode->setDeps(appendVerticesGroupNode->dependencies());

  newJoin->setLeftVar(join->leftInputVar());
  newJoin->setRightVar(newProject->outputVar());
  newJoin->setOutputVar(join->outputVar());
  auto* newJoinGroupNode = OptGroupNode::create(octx, newJoin, joinGroup);
  newJoinGroupNode->dependsOn(joinGroupNode->dependencies()[0]);
  newJoinGroupNode->dependsOn(newProjectGroup);

  result.newGroupNodes.emplace_back(newJoinGroupNode);
  return result;
}

std::string RemoveAppendVerticesBelowJoinRule::toString() const {
  return "RemoveAppendVerticesBelowJoinRule";
}

}  // namespace opt
}  // namespace nebula
