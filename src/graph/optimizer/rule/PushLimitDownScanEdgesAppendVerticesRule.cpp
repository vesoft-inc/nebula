/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushLimitDownScanEdgesAppendVerticesRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::AppendVertices;
using nebula::graph::Limit;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;
using nebula::graph::ScanEdges;

namespace nebula {
namespace opt {

// Limit->AppendVertices->Project->ScanEdges ==> Limit->AppendVertices->Project->ScanEdges(Limit)

std::unique_ptr<OptRule> PushLimitDownScanEdgesAppendVerticesRule::kInstance =
    std::unique_ptr<PushLimitDownScanEdgesAppendVerticesRule>(
        new PushLimitDownScanEdgesAppendVerticesRule());

PushLimitDownScanEdgesAppendVerticesRule::PushLimitDownScanEdgesAppendVerticesRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushLimitDownScanEdgesAppendVerticesRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kLimit,
      {Pattern::create(graph::PlanNode::Kind::kAppendVertices,
                       {Pattern::create(graph::PlanNode::Kind::kProject,
                                        {Pattern::create(graph::PlanNode::Kind::kScanEdges)})})});
  return pattern;
}

bool PushLimitDownScanEdgesAppendVerticesRule::match(OptContext *ctx,
                                                     const MatchedResult &matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto av = static_cast<const AppendVertices *>(matched.planNode({0, 0}));
  auto *src = av->src();
  auto *inputExpr = graph::ExpressionUtils::findAny(
      src, {Expression::Kind::kInputProperty, Expression::Kind::kVarProperty});
  if (inputExpr == nullptr) {
    return false;
  }
  auto *filter = av->filter();
  auto *vFilter = av->vFilter();
  // Limit can't push over filter operation
  return filter == nullptr && vFilter == nullptr;
}

StatusOr<OptRule::TransformResult> PushLimitDownScanEdgesAppendVerticesRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto limitGroupNode = matched.node;
  auto appendVerticesGroupNode = matched.dependencies.front().node;
  auto projGroupNode = matched.dependencies.front().dependencies.front().node;
  auto scanEdgesGroupNode =
      matched.dependencies.front().dependencies.front().dependencies.front().node;

  const auto limit = static_cast<const Limit *>(limitGroupNode->node());
  const auto appendVertices = static_cast<const AppendVertices *>(appendVerticesGroupNode->node());
  const auto proj = static_cast<const Project *>(projGroupNode->node());
  const auto scanEdges = static_cast<const ScanEdges *>(scanEdgesGroupNode->node());

  int64_t limitRows = limit->offset() + limit->count();
  if (scanEdges->limit() >= 0 && limitRows >= scanEdges->limit()) {
    return TransformResult::noTransform();
  }

  auto newLimit = static_cast<Limit *>(limit->clone());
  newLimit->setOutputVar(limit->outputVar());
  auto newLimitGroupNode = OptGroupNode::create(octx, newLimit, limitGroupNode->group());

  auto newAppendVertices = static_cast<AppendVertices *>(appendVertices->clone());
  auto newAppendVerticesGroup = OptGroup::create(octx);
  auto newAppendVerticesGroupNode = newAppendVerticesGroup->makeGroupNode(newAppendVertices);

  auto newProj = static_cast<Project *>(proj->clone());
  auto newProjGroup = OptGroup::create(octx);
  auto newProjGroupNode = newProjGroup->makeGroupNode(newProj);

  auto newScanEdges = static_cast<ScanEdges *>(scanEdges->clone());
  newScanEdges->setLimit(limitRows);
  auto newScanEdgesGroup = OptGroup::create(octx);
  auto newScanEdgesGroupNode = newScanEdgesGroup->makeGroupNode(newScanEdges);

  newLimitGroupNode->dependsOn(newAppendVerticesGroup);
  newLimit->setInputVar(newAppendVertices->outputVar());
  newAppendVerticesGroupNode->dependsOn(newProjGroup);
  newAppendVertices->setInputVar(newProj->outputVar());
  newProjGroupNode->dependsOn(newScanEdgesGroup);
  newProj->setInputVar(newScanEdges->outputVar());
  for (auto dep : scanEdgesGroupNode->dependencies()) {
    newScanEdgesGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newLimitGroupNode);
  return result;
}

std::string PushLimitDownScanEdgesAppendVerticesRule::toString() const {
  return "PushLimitDownScanEdgesAppendVerticesRule";
}

}  // namespace opt
}  // namespace nebula
