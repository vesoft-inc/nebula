/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownFulltextIndexScanRule2.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Argument;
using nebula::graph::Explore;
using nebula::graph::FulltextIndexScan;
using nebula::graph::HashInnerJoin;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushLimitDownFulltextIndexScanRule2::kInstance =
    std::unique_ptr<PushLimitDownFulltextIndexScanRule2>(new PushLimitDownFulltextIndexScanRule2());

PushLimitDownFulltextIndexScanRule2::PushLimitDownFulltextIndexScanRule2() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownFulltextIndexScanRule2::pattern() const {
  static Pattern pattern = Pattern::create(
      PlanNode::Kind::kLimit,
      {
          Pattern::create(
              PlanNode::Kind::kHashInnerJoin,
              {
                  Pattern::create(PlanNode::Kind::kFulltextIndexScan),
                  Pattern::create({PlanNode::Kind::kGetVertices, PlanNode::Kind::kGetEdges},
                                  {
                                      Pattern::create(PlanNode::Kind::kArgument),
                                  }),
              }),
      });
  return pattern;
}

bool PushLimitDownFulltextIndexScanRule2::match(OptContext *, const MatchedResult &matched) const {
  auto ft = static_cast<const FulltextIndexScan *>(matched.planNode({0, 0, 0}));
  return !ft->limitExpr() || ft->limit() < 0 || ft->offset() < 0;
}

StatusOr<OptRule::TransformResult> PushLimitDownFulltextIndexScanRule2::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.result().node;
  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  auto newLimit = static_cast<Limit *>(limit->clone());
  newLimit->setOutputVar(limit->outputVar());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto joinGroupNode = matched.result({0, 0}).node;
  auto join = static_cast<const HashInnerJoin *>(joinGroupNode->node());
  auto newJoin = static_cast<HashInnerJoin *>(join->clone());
  auto newJoinGroup = OptGroup::create(octx);
  auto newJoinGroupNode = newJoinGroup->makeGroupNode(newJoin);

  newLimitGroupNode->dependsOn(newJoinGroup);
  newLimit->setInputVar(newJoin->outputVar());

  auto ftGroupNode = matched.result({0, 0, 0}).node;
  const auto ft = static_cast<const FulltextIndexScan *>(ftGroupNode->node());
  auto newFt = static_cast<FulltextIndexScan *>(ft->clone());
  newFt->setLimit(limit->count() + limit->offset());
  newFt->setOffset(limit->offset());
  auto newFtGroup = OptGroup::create(octx);
  auto newFtGroupNode = newFtGroup->makeGroupNode(newFt);

  newJoinGroupNode->dependsOn(newFtGroup);
  for (auto dep : ftGroupNode->dependencies()) {
    newFtGroupNode->dependsOn(dep);
  }

  auto exploreGroupNode = matched.result({0, 0, 1}).node;
  auto explore = static_cast<const Explore *>(exploreGroupNode->node());
  auto newExplore = static_cast<Explore *>(explore->clone());
  auto newExploreGroup = OptGroup::create(octx);
  auto newExploreGroupNode = newExploreGroup->makeGroupNode(newExplore);

  newJoinGroupNode->dependsOn(newExploreGroup);

  auto argGroupNode = matched.result({0, 0, 1, 0}).node;
  auto arg = static_cast<const Argument *>(argGroupNode->node());
  auto newArg = static_cast<Argument *>(arg->clone());
  auto newArgGroup = OptGroup::create(octx);
  auto newArgGroupNode = newArgGroup->makeGroupNode(newArg);

  newExploreGroupNode->dependsOn(newArgGroup);
  newExplore->setInputVar(newArg->outputVar());
  for (auto dep : argGroupNode->dependencies()) {
    newArgGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownFulltextIndexScanRule2::toString() const {
  return "PushLimitDownFulltextIndexScanRule2";
}

}  // namespace opt
}  // namespace nebula
