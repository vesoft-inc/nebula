/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownProjectRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

// transform Limit->Project to Project->Limit
std::unique_ptr<OptRule> PushLimitDownProjectRule::kInstance =
    std::unique_ptr<PushLimitDownProjectRule>(new PushLimitDownProjectRule());

PushLimitDownProjectRule::PushLimitDownProjectRule() {
  RuleSet::QueryRules().addRule(this);
}

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
  auto projInputVar = proj->inputVar();
  newLimit->setOutputVar(proj->outputVar());
  newLimit->setInputVar(projInputVar);
  auto *varPtr = octx->qctx()->symTable()->getVar(projInputVar);
  DCHECK(!!varPtr);
  newLimit->setColNames(varPtr->colNames);

  auto newProj = static_cast<Project *>(proj->clone());
  auto newProjGroupNode = OptGroupNode::create(octx, newProj, limitGroupNode->group());
  newProj->setOutputVar(limit->outputVar());
  newProj->setInputVar(newLimit->outputVar());

  newProjGroupNode->dependsOn(const_cast<OptGroup *>(newLimitGroupNode->group()));
  for (auto dep : projGroupNode->dependencies()) {
    newLimitGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newProjGroupNode);
  return result;
}

std::string PushLimitDownProjectRule::toString() const {
  return "PushLimitDownProjectRule";
}

}  // namespace opt
}  // namespace nebula
