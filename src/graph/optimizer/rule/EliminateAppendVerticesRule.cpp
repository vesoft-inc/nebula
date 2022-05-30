// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/optimizer/rule/EliminateAppendVerticesRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::AppendVertices;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

// TODO If we prune properties in Optimizer Rules, we could check properties of AppendVertices,
// and drop AppendVertices if it has no properties directly.
std::unique_ptr<OptRule> EliminateAppendVerticesRule::kInstance =
    std::unique_ptr<EliminateAppendVerticesRule>(new EliminateAppendVerticesRule());

EliminateAppendVerticesRule::EliminateAppendVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& EliminateAppendVerticesRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kProject, {Pattern::create(graph::PlanNode::Kind::kAppendVertices)});
  return pattern;
}

bool EliminateAppendVerticesRule::match(OptContext* octx, const MatchedResult& matched) const {
  if (!OptRule::match(octx, matched)) {
    return false;
  }
  const auto* project = static_cast<const Project*>(matched.planNode({0}));
  DCHECK_EQ(project->kind(), graph::PlanNode::Kind::kProject);
  for (const auto& col : project->columns()->columns()) {
    const auto* expr = graph::ExpressionUtils::findAny(col->expr(), {Expression::Kind::kPathBuild});
    if (expr != nullptr) {
      return false;
    }
  }
  const auto* appendVertices = static_cast<const AppendVertices*>(matched.planNode({0, 0}));
  DCHECK_EQ(appendVertices->kind(), graph::PlanNode::Kind::kAppendVertices);
  if (appendVertices->vFilter() != nullptr || appendVertices->filter() != nullptr) {
    return false;
  }
  if (!graph::AnonVarGenerator::isAnnoVar(appendVertices->colNames().back())) {  // Anonymous node
    return false;
  }
  return true;
}

StatusOr<OptRule::TransformResult> EliminateAppendVerticesRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto projectGroupNode = matched.node;
  auto appendVerticesGroupNode = matched.dependencies.front().node;

  const auto project = static_cast<const Project*>(projectGroupNode->node());
  const auto appendVertices = static_cast<const AppendVertices*>(appendVerticesGroupNode->node());

  auto newProj = static_cast<Project*>(project->clone());
  newProj->setOutputVar(project->outputVar());
  newProj->setInputVar(appendVertices->inputVar());
  auto newProjGroupNode = OptGroupNode::create(octx, newProj, projectGroupNode->group());
  newProjGroupNode->setDeps(appendVerticesGroupNode->dependencies());

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newProjGroupNode);
  return result;
}

std::string EliminateAppendVerticesRule::toString() const {
  return "EliminateAppendVerticesRule";
}

}  // namespace opt
}  // namespace nebula
