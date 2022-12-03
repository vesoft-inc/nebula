/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

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
      PlanNode::Kind::kBiLeftJoin,
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
  auto* leftJoin = static_cast<graph::BiLeftJoin*>(leftJoinGroupNode->node());

  auto* projectGroupNode = matched.dependencies[1].node;
  auto* projectGroup = projectGroupNode->group();
  UNUSED(projectGroup);

  auto* project = static_cast<graph::Project*>(projectGroupNode->node());

  auto* appendVerticesGroup = matched.dependencies[1].dependencies[0].node->group();
  UNUSED(appendVerticesGroup);
  auto appendVertices =
      static_cast<graph::AppendVertices*>(matched.dependencies[1].dependencies[0].node->node());

  auto* traverseGroup = matched.dependencies[1].dependencies[0].node->group();
  UNUSED(traverseGroup);
  auto traverse = static_cast<graph::Traverse*>(
      matched.dependencies[1].dependencies[0].dependencies[0].node->node());

  auto& avColNames = appendVertices->colNames();
  DCHECK_GE(avColNames.size(), 1);
  auto& avNodeAlias = avColNames.back();

  auto& tvColNames = traverse->colNames();
  DCHECK_GE(tvColNames.size(), 1);
  auto& tvEdgeAlias = traverse->colNames().back();

  auto& hashKeys = leftJoin->hashKeys();
  auto& probeKeys = leftJoin->probeKeys();

  // Use visitor to collect all function `id` in the hashKeys

  std::vector<size_t> hashKeyIdx;
  for (size_t i = 0; i < hashKeys.size(); ++i) {
    auto* key = hashKeys[i];
    if (key->kind() != Expression::Kind::kFunctionCall) {
      continue;
    }
    auto* func = static_cast<FunctionCallExpression*>(key);
    if (func->name() != "id" || func->name() != "_joinkey") {
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
    // FIXME(jie): Must check if probe keys contain the same key
    hashKeyIdx.emplace_back(i);
  }
  if (hashKeyIdx.size() != 1) {
    return TransformResult::noTransform();
  }

  std::vector<size_t> prjIdx;
  for (size_t i = 0; i < project->columns()->size(); ++i) {
    const auto* col = project->columns()->columns()[i];
    if (col->expr()->kind() != Expression::Kind::kInputProperty) {
      continue;
    }
    auto* inputProp = static_cast<InputPropertyExpression*>(col->expr());
    if (inputProp->prop() != avNodeAlias) continue;
    prjIdx.push_back(i);
  }
  if (prjIdx.size() != 1) {
    return TransformResult::noTransform();
  }

  auto* pool = octx->qctx()->objPool();
  // Let the new project generate expr `none_direct_dst($-.tvEdgeAlias)`, and let the new left join
  // use it as hash key
  auto* args = ArgumentList::make(pool);
  args->addArgument(InputPropertyExpression::make(pool, tvEdgeAlias));
  auto* newPrjExpr = FunctionCallExpression::make(pool, "none_direct_dst", args);

  auto* newYieldColumns = pool->makeAndAdd<YieldColumns>();
  for (size_t i = 0; i < project->columns()->size(); ++i) {
    if (i == prjIdx[0]) {
      newYieldColumns->addColumn(pool->makeAndAdd<YieldColumn>(newPrjExpr, newPrjExpr->toString()));
    } else {
      newYieldColumns->addColumn(project->columns()->columns()[i]);
    }
  }
  auto* newProject = graph::Project::make(octx->qctx(), nullptr, newYieldColumns);

  auto* newHashExpr = InputPropertyExpression::make(pool, newPrjExpr->toString());
  std::vector<Expression*> newHashKeys;
  for (size_t i = 0; i < hashKeys.size(); ++i) {
    if (i == hashKeyIdx[0]) {
      newHashKeys.emplace_back(newHashExpr);
    } else {
      newHashKeys.emplace_back(hashKeys[i]);
    }
  }
  auto* newLeftJoin =
      graph::BiLeftJoin::make(octx->qctx(), nullptr, nullptr, newHashKeys, probeKeys);

  TransformResult result;
  result.eraseAll = true;

  newProject->setInputVar(appendVertices->inputVar());
  newProject->setOutputVar(project->outputVar());
  auto newProjectGroup = OptGroup::create(octx);
  auto* newProjectGroupNode = newProjectGroup->makeGroupNode(newProject);
  newProjectGroupNode->setDeps(projectGroupNode->dependencies());

  newLeftJoin->setDep(1, newProject);
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
