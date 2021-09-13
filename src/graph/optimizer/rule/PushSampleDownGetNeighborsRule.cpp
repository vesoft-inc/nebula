/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/optimizer/rule/PushSampleDownGetNeighborsRule.h"

#include "common/expression/BinaryExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/UnaryExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/visitor/ExtractFilterExprVisitor.h"

using nebula::graph::GetNeighbors;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;
using nebula::graph::Sample;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushSampleDownGetNeighborsRule::kInstance =
    std::unique_ptr<PushSampleDownGetNeighborsRule>(new PushSampleDownGetNeighborsRule());

PushSampleDownGetNeighborsRule::PushSampleDownGetNeighborsRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushSampleDownGetNeighborsRule::pattern() const {
  static Pattern pattern =
      Pattern::create(graph::PlanNode::Kind::kSample,
                      {Pattern::create(graph::PlanNode::Kind::kProject,
                                       {Pattern::create(graph::PlanNode::Kind::kGetNeighbors)})});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushSampleDownGetNeighborsRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto sampleGroupNode = matched.node;
  auto projGroupNode = matched.dependencies.front().node;
  auto gnGroupNode = matched.dependencies.front().dependencies.front().node;

  const auto sample = static_cast<const Sample *>(sampleGroupNode->node());
  const auto proj = static_cast<const Project *>(projGroupNode->node());
  const auto gn = static_cast<const GetNeighbors *>(gnGroupNode->node());

  int64_t sampleRows = sample->count();
  if (gn->limit() >= 0 && sampleRows >= gn->limit()) {
    return TransformResult::noTransform();
  }

  auto newSample = static_cast<Sample *>(sample->clone());
  auto newSampleGroupNode = OptGroupNode::create(octx, newSample, sampleGroupNode->group());

  auto newProj = static_cast<Project *>(proj->clone());
  auto newProjGroup = OptGroup::create(octx);
  auto newProjGroupNode = newProjGroup->makeGroupNode(newProj);

  auto newGn = static_cast<GetNeighbors *>(gn->clone());
  newGn->setLimit(sampleRows);
  newGn->setRandom(true);
  auto newGnGroup = OptGroup::create(octx);
  auto newGnGroupNode = newGnGroup->makeGroupNode(newGn);

  newSampleGroupNode->dependsOn(newProjGroup);
  newProjGroupNode->dependsOn(newGnGroup);
  for (auto dep : gnGroupNode->dependencies()) {
    newGnGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newSampleGroupNode);
  return result;
}

std::string PushSampleDownGetNeighborsRule::toString() const {
  return "PushSampleDownGetNeighborsRule";
}

}  // namespace opt
}  // namespace nebula
