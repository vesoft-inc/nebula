/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/RemoveProjectDedupBeforeGetDstBySrcRule.h"

#include "common/expression/ColumnExpression.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> RemoveProjectDedupBeforeGetDstBySrcRule::kInstance =
    std::unique_ptr<RemoveProjectDedupBeforeGetDstBySrcRule>(
        new RemoveProjectDedupBeforeGetDstBySrcRule());

RemoveProjectDedupBeforeGetDstBySrcRule::RemoveProjectDedupBeforeGetDstBySrcRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& RemoveProjectDedupBeforeGetDstBySrcRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kGetDstBySrc,
      {Pattern::create(graph::PlanNode::Kind::kDedup,
                       {Pattern::create(graph::PlanNode::Kind::kProject,
                                        {Pattern::create(graph::PlanNode::Kind::kDataCollect)})})});
  return pattern;
}

StatusOr<OptRule::TransformResult> RemoveProjectDedupBeforeGetDstBySrcRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  auto* getDstBySrcGroupNode = matched.node;
  auto* getDstBySrc = static_cast<const graph::GetDstBySrc*>(getDstBySrcGroupNode->node());
  auto* projectGroupNode = matched.dependencies.front().dependencies.front().node;
  auto* project = static_cast<const graph::Project*>(projectGroupNode->node());

  auto* newGetDstBySrc = static_cast<graph::GetDstBySrc*>(getDstBySrc->clone());
  // Replace `$-._vid` with `COLUMN[0]`
  newGetDstBySrc->setSrc(ColumnExpression::make(getDstBySrc->src()->getObjPool(), 0));
  newGetDstBySrc->setOutputVar(getDstBySrc->outputVar());
  newGetDstBySrc->setInputVar(project->inputVar());
  newGetDstBySrc->setColNames(newGetDstBySrc->colNames());

  auto newGetDstBySrcNode =
      OptGroupNode::create(octx, newGetDstBySrc, getDstBySrcGroupNode->group());
  for (auto dep : projectGroupNode->dependencies()) {
    newGetDstBySrcNode->dependsOn(dep);
  }

  TransformResult result;
  result.newGroupNodes.emplace_back(newGetDstBySrcNode);
  result.eraseAll = true;
  return result;
}

bool RemoveProjectDedupBeforeGetDstBySrcRule::match(OptContext*,
                                                    const MatchedResult& matched) const {
  auto* getDstBySrc = static_cast<const graph::GetDstBySrc*>(matched.node->node());
  auto* project = static_cast<const graph::Project*>(
      matched.dependencies.front().dependencies.front().node->node());
  auto* dataCollect = static_cast<const graph::DataCollect*>(
      matched.dependencies.front().dependencies.front().dependencies.front().node->node());

  // src should be $-._vid
  auto* expr = getDstBySrc->src();
  if (expr->kind() != Expression::Kind::kInputProperty ||
      static_cast<InputPropertyExpression*>(expr)->prop() != "_vid") {
    return false;
  }
  if (project->columns()->size() != 1) {
    return false;
  }
  if (dataCollect->kind() != graph::DataCollect::DCKind::kMToN || !dataCollect->distinct() ||
      dataCollect->colNames().size() != 1) {
    return false;
  }

  return true;
}

std::string RemoveProjectDedupBeforeGetDstBySrcRule::toString() const {
  return "RemoveProjectDedupBeforeGetDstBySrcRule";
}

}  // namespace opt
}  // namespace nebula
