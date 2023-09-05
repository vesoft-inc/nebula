/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownAllPathsRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::AllPaths;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

// transform Limit->AllPaths to AllPaths(limit)
std::unique_ptr<OptRule> PushLimitDownAllPathsRule::kInstance =
    std::unique_ptr<PushLimitDownAllPathsRule>(new PushLimitDownAllPathsRule());

PushLimitDownAllPathsRule::PushLimitDownAllPathsRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownAllPathsRule::pattern() const {
  static Pattern pattern = Pattern::create(graph::PlanNode::Kind::kLimit,
                                           {Pattern::create(graph::PlanNode::Kind::kAllPaths)});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushLimitDownAllPathsRule::transform(
    OptContext *ctx, const MatchedResult &matched) const {
  auto qctx = ctx->qctx();
  auto limitGroupNode = matched.node;
  auto pathGroupNode = matched.dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto path = static_cast<const AllPaths *>(pathGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count(qctx);
  if (path->limit() >= 0 && limitRows >= path->limit()) {
    return TransformResult::noTransform();
  }

  auto newPath = static_cast<AllPaths *>(path->clone());
  OptGroupNode *newPathGroupNode = nullptr;
  // Limit<-AllPaths => AllPaths(limit)
  newPathGroupNode = OptGroupNode::create(ctx, newPath, limitGroupNode->group());
  newPath->setOffset(limit->offset());
  newPath->setLimit(limitRows);
  newPath->setOutputVar(limit->outputVar());

  for (auto dep : pathGroupNode->dependencies()) {
    newPathGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newPathGroupNode);
  return result;
}

std::string PushLimitDownAllPathsRule::toString() const {
  return "PushLimitDownAllPathsRule";
}

}  // namespace opt
}  // namespace nebula
