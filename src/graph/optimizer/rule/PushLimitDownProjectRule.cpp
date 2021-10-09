/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/optimizer/rule/PushLimitDownProjectRule.h"

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

using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

// transform Limit->Project to Project->Limit
std::unique_ptr<OptRule> PushLimitDownProjectRule::kInstance =
    std::unique_ptr<PushLimitDownProjectRule>(new PushLimitDownProjectRule());

PushLimitDownProjectRule::PushLimitDownProjectRule() { RuleSet::QueryRules().addRule(this); }

const Pattern &PushLimitDownProjectRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kProject)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownProjectRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto projGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto proj = static_cast<const Project *>(projGroupNode->node());

  auto newLimit = static_cast<Limit *>(limit->clone());
  auto newLimitGroup = OptGroup::create(octx);
  auto newLimitGroupNode = newLimitGroup->makeGroupNode(newLimit);
  newLimit->setOutputVar(proj->outputVar());
  newLimit->setInputVar(proj->inputVar());
  //  newLimit->setColNames(proj->colNames());

  auto newProj = static_cast<Project *>(proj->clone());
  auto newProjGroupNode = OptGroupNode::create(octx, newProj, limitGroupNode->group());
  newProj->setOutputVar(limit->outputVar());
  newProj->setInputVar(newLimit->outputVar());
  //  newProj->setColNames(limit->colNames());

  newProjGroupNode->dependsOn(const_cast<OptGroup *>(newLimitGroupNode->group()));
  for (auto dep : projGroupNode->dependencies()) {
    newLimitGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newProjGroupNode);
  return result;
}

std::string PushLimitDownProjectRule::toString() const { return "PushLimitDownProjectRule"; }

}  // namespace opt
}  // namespace nebula
