/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownScanAppendVerticesRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::AppendVertices;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::ScanVertices;

namespace nebula {
namespace opt {

// Limit->AppendVertices->ScanVertices ==> Limit->AppendVertices->ScanVertices(Limit)

std::unique_ptr<OptRule> PushLimitDownScanAppendVerticesRule::kInstance =
    std::unique_ptr<PushLimitDownScanAppendVerticesRule>(new PushLimitDownScanAppendVerticesRule());

PushLimitDownScanAppendVerticesRule::PushLimitDownScanAppendVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownScanAppendVerticesRule::pattern() const {
  static Pattern pattern =
      Pattern::create(graph::PlanNode::Kind::kLimit,
                      {Pattern::create(graph::PlanNode::Kind::kAppendVertices,
                                       {Pattern::create(graph::PlanNode::Kind::kScanVertices)})});
  return pattern;
}

bool PushLimitDownScanAppendVerticesRule::match(OptContext *ctx,
                                                const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto av = static_cast<const AppendVertices *>(matched.planNode({0, 0}));
  auto *src = av->src();
  if (src->kind() != Expression::Kind::kInputProperty &&
      src->kind() != Expression::Kind::kVarProperty) {
    return false;
  }
  auto *propExpr = static_cast<const PropertyExpression *>(src);
  if (propExpr->prop() != kVid) {
    return false;
  }
  auto *filter = av->filter();
  auto *vFilter = av->vFilter();
  // Limit can't push over filter operation
  return filter == nullptr && vFilter == nullptr;
}

StatusOr<OptRule::TransformResult> PushLimitDownScanAppendVerticesRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto appendVerticesGroupNode = matched.dependencies.front().node;
  auto scanVerticesGroupNode = matched.dependencies.front().dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  const auto scanVertices = static_cast<const ScanVertices *>(scanVerticesGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count();
  if (scanVertices->limit() >= 0 && limitRows >= scanVertices->limit()) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  newLimit->setOutputVar(limit->outputVar());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newAppendVertices = static_cast<AppendVertices *>(appendVertices->clone());
  auto newAppendVerticesGroup = OptGroup::create(octx);
  auto newAppendVerticesGroupNode = newAppendVerticesGroup->makeGroupNode(newAppendVertices);

  auto newScanVertices = static_cast<ScanVertices *>(scanVertices->clone());
  newScanVertices->setLimit(limitRows);
  auto newScanVerticesGroup = OptGroup::create(octx);
  auto newScanVerticesGroupNode = newScanVerticesGroup->makeGroupNode(newScanVertices);

  newLimitGroupNode->dependsOn(newAppendVerticesGroup);
  newLimit->setInputVar(newAppendVertices->outputVar());
  newAppendVerticesGroupNode->dependsOn(newScanVerticesGroup);
  newAppendVertices->setInputVar(newScanVertices->outputVar());
  for (auto dep : scanVerticesGroupNode->dependencies()) {
    newScanVerticesGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownScanAppendVerticesRule::toString() const {
  return "PushLimitDownScanAppendVerticesRule";
}

}  // namespace opt
}  // namespace nebula
