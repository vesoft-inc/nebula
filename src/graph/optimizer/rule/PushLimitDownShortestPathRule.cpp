/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownShortestPathRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::BFSShortestPath;
using nebula::graph::Limit;
using nebula::graph::MultiShortestPath;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {
// transform Limit->DataCollect->Loop->BFS/MultiShortest
// to DataCollect->Loop->BFS/MultiShortest(limit)
std::unique_ptr<OptRule> PushLimitDownShortestPathRule::kInstance =
    std::unique_ptr<PushLimitDownShortestPathRule>(new PushLimitDownShortestPathRule());

PushLimitDownShortestPathRule::PushLimitDownShortestPathRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownShortestPathRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit,
      {Pattern::create(
          graph::PlanNode::Kind::kDataCollect,
          {Pattern::create(graph::PlanNode::Kind::kLoop,
                           {Pattern::create({graph::PlanNode::Kind::kBFSShortest,
                                             graph::PlanNode::Kind::kMultiShortestPath})})})});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownShortestPathRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto qctx = ctx->qctx();
  auto limitGroupNode = matched.node;
  auto dataCollectGroupNode = matched.dependencies.front().node;
  auto loopGroupNode = matched.dependencies.front().dependencies.front().node;
  auto pathGroupNode = matched.dependencies.front().dependencies.front().dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto dataCollect = static_cast<const DataCollect *>(dataCollectGroupNode->node());
  const auto loop = static_cast<const Loop *>(loopGroupNode->node());
  auto pathNode = pathGroupNode->node();
  auto pathKind = pathNode->kind();

  int64_t pathLimit = -1;
  if (pathKind == PlanNode::Kind::kBFSShortest) {
    pathLimit = static_cast<const BFSShortestPath *>(pathNode)->limit();
  } else {
    pathLimit = static_cast<const MultiShortestPath *>(pathNode)->limit();
  }

  int64_t limitRows = limit->offset() + limit->count(qctx);
  if (pathLimit >= 0 && limitRows >= pathLimit) {
    return TransformResult::noTransform();
  }

  auto newDataCollect = static_cast<dataCollect *>(dataCollect->clone());
  newDataCollect->setOutputVar(limit->outputVar());
  auto newDataCollectGroupNode = OptGroupNode::create(ctx, newDataCollect, limitGroupNode->group());

  auto newLoop = static_cast<Loop *>(loop->clone());
  auto newLoopGroup = OptGroup::create(ctx);
  auto newLoopGroupNode = newLoopGroup->makeGroupNode(newLoop);

  auto newPath = static_cast<decltype(pathNode)>(pathNode->clone());
  if (pathKind == PlanNode::Kind::kBFSShortest) {
    static_cast<BFSShortestPath *>(newPath)->setLimit(limitRows);
  } else {
    static_cast<MultiShortestPath *>(newPath)->setLimit(limitRows);
  }
  auto newPathGroup = OptGroup::create(ctx);
  auto newPathGroupNode = newPathGroup->makeGroupNode(newPath);

  newDataCollectGroupNode->dependsOn(newLoopGroup);

  for (auto dep : pathGroupNode->dependencies()) {
    newPathGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newPathGroupNode);
  return result;
}

std::string PushLimitDownShortestPathRule::toString() const {
  return "PushLimitDownShortestPathRule";
}

}  // namespace opt
}  // namespace nebula
