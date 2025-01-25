/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownVectorIndexScanRule2.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Argument;
using nebula::graph::Explore;
using nebula::graph::VectorIndexScan;
using nebula::graph::HashInnerJoin;
using nebula::graph::Limit;
using nebula::graph::Project;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownVectorIndexScanRule2::kInstance =
    std::unique_ptr<PushLimitDownVectorIndexScanRule2>(new PushLimitDownVectorIndexScanRule2());

PushLimitDownVectorIndexScanRule2::PushLimitDownVectorIndexScanRule2() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& PushLimitDownVectorIndexScanRule2::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kLimit,
      {Pattern::create(
          PlanNode::Kind::kHashInnerJoin,
          {Pattern::create(PlanNode::Kind::kVectorIndexScan),
           Pattern::create(
               PlanNode::Kind::kProject,
               {Pattern::create(
                   {PlanNode::Kind::kGetVertices, PlanNode::Kind::kGetEdges},
                   {Pattern::create(PlanNode::Kind::kArgument)})})})});
  return pattern;
}

bool PushLimitDownVectorIndexScanRule2::match(OptContext*, const MatchedResult& matched) const {
  auto vector = static_cast<const VectorIndexScan*>(matched.planNode({0, 0, 0}));
  return !vector->limitExpr() || vector->limit() < 0 || vector->offset() < 0;
}

StatusOr<OptRule::TransformResult> PushLimitDownVectorIndexScanRule2::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto limitGroupNode = matched.result().node;
  const auto limit = static_cast<const Limit*>(limitGroupNode->node());

  auto joinGroupNode = matched.result({0, 0}).node;
  auto join = static_cast<const HashInnerJoin*>(joinGroupNode->node());
  auto newJoin = static_cast<HashInnerJoin*>(join->clone());
  auto newJoinGroupNode = OptGroupNode::create(octx, newJoin, limitGroupNode->group());

  newJoin->setOutputVar(limit->outputVar());

  auto vectorGroupNode = matched.result({0, 0, 0}).node;
  const auto vector = static_cast<const VectorIndexScan*>(vectorGroupNode->node());
  auto newVector = static_cast<VectorIndexScan*>(vector->clone());
  newVector->setLimit(limit->count() + limit->offset());
  newVector->setOffset(limit->offset());
  auto newVectorGroup = OptGroup::create(octx);
  auto newVectorGroupNode = newVectorGroup->makeGroupNode(newVector);

  newJoinGroupNode->dependsOn(newVectorGroup);
  newJoin->setLeftVar(newVector->outputVar());
  for (auto dep : vectorGroupNode->dependencies()) {
    newVectorGroupNode->dependsOn(dep);
  }

  auto projGroupNode = matched.result({0, 0, 1}).node;
  auto proj = static_cast<const Project*>(projGroupNode->node());
  auto newProj = static_cast<Project*>(proj->clone());
  auto newProjGroup = OptGroup::create(octx);
  auto newProjGroupNode = newProjGroup->makeGroupNode(newProj);

  newJoinGroupNode->dependsOn(newProjGroup);
  newJoin->setRightVar(newProj->outputVar());

  auto exploreGroupNode = matched.result({0, 0, 1, 0}).node;
  auto explore = static_cast<const Explore*>(exploreGroupNode->node());
  auto newExplore = static_cast<Explore*>(explore->clone());
  auto newExploreGroup = OptGroup::create(octx);
  auto newExploreGroupNode = newExploreGroup->makeGroupNode(newExplore);

  newProjGroupNode->dependsOn(newExploreGroup);
  newProj->setInputVar(newExplore->outputVar());

  auto argGroupNode = matched.result({0, 0, 1, 0, 0}).node;
  auto arg = static_cast<const Argument*>(argGroupNode->node());
  auto newArg = static_cast<Argument*>(arg->clone());
  auto newArgGroup = OptGroup::create(octx);
  auto newArgGroupNode = newArgGroup->makeGroupNode(newArg);

  newExploreGroupNode->dependsOn(newArgGroup);
  newExplore->setInputVar(newArg->outputVar());
  for (auto dep : argGroupNode->dependencies()) {
    newArgGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newJoinGroupNode);
  return result;
}

std::string PushLimitDownVectorIndexScanRule2::toString() const {
  return "PushLimitDownVectorIndexScanRule2";
}

}  // namespace opt
}  // namespace nebula 