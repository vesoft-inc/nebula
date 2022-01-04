/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/PushTopNDownEdgeIndexFullScanRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/planner/plan/Scan.h"

using nebula::graph::EdgeIndexFullScan;
using nebula::graph::PlanNode;
using nebula::graph::Project;
using nebula::graph::QueryContext;
using nebula::graph::TopN;

namespace nebula {
namespace opt {

std::unique_ptr<OptRule> PushTopNDownEdgeIndexFullScanRule::kInstance =
    std::unique_ptr<PushTopNDownEdgeIndexFullScanRule>(new PushTopNDownEdgeIndexFullScanRule());

PushTopNDownEdgeIndexFullScanRule::PushTopNDownEdgeIndexFullScanRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern &PushTopNDownEdgeIndexFullScanRule::pattern() const {
  static Pattern pattern = Pattern::create(
      graph::PlanNode::Kind::kTopN,
      {Pattern::create(graph::PlanNode::Kind::kProject,
                       {Pattern::create(graph::PlanNode::Kind::kEdgeIndexFullScan)})});
  return pattern;
}

StatusOr<OptRule::TransformResult> PushTopNDownEdgeIndexFullScanRule::transform(
    OptContext *octx, const MatchedResult &matched) const {
  auto topNGroupNode = matched.node;
  auto projectGroupNode = matched.dependencies.front().node;
  auto indexScanGroupNode = matched.dependencies.front().dependencies.front().node;

  const auto topN = static_cast<const TopN *>(topNGroupNode->node());
  const auto project = static_cast<const Project *>(projectGroupNode->node());
  const auto indexScan = static_cast<const EdgeIndexFullScan *>(indexScanGroupNode->node());

  int64_t limitRows = topN->offset() + topN->count();

  auto &factors = topN->factors();
  auto projColNames = project->colNames();
  const auto &yieldColumns = indexScan->yieldColumns();

  std::unordered_map<std::string, std::string> namesMap;
  for (auto yieldColumn : yieldColumns->columns()) {
    if (yieldColumn->expr()->kind() == Expression::Kind::kEdgeProperty) {
      const auto &propName = static_cast<EdgePropertyExpression *>(yieldColumn->expr())->prop();
      namesMap[yieldColumn->name()] = propName;
      continue;
    }
    return TransformResult::noTransform();
  }
  std::vector<storage::cpp2::OrderBy> orderBys;
  orderBys.reserve(factors.size());

  for (auto factor : factors) {
    auto colName = projColNames[factor.first];
    auto found = namesMap.find(colName);
    if (found == namesMap.end()) {
      return Status::Error();
    }
    storage::cpp2::OrderBy orderBy;
    orderBy.prop_ref() = found->second;
    orderBy.direction_ref() = factor.second == OrderFactor::OrderType::ASCEND
                                  ? storage::cpp2::OrderDirection::ASCENDING
                                  : storage::cpp2::OrderDirection::DESCENDING;
    orderBys.emplace_back(orderBy);
  }

  auto newTopN = static_cast<TopN *>(topN->clone());
  auto newtopNGroupNode = OptGroupNode::create(octx, newTopN, topNGroupNode->group());

  auto newProject = static_cast<Project *>(project->clone());
  auto newProjectGroup = OptGroup::create(octx);
  auto newProjectGroupNode = newProjectGroup->makeGroupNode(newProject);

  auto newIndexScan = static_cast<EdgeIndexFullScan *>(indexScan->clone());
  newIndexScan->setLimit(limitRows);
  newIndexScan->setOrderBy(orderBys);
  auto newIndexScanGroup = OptGroup::create(octx);
  auto newIndexScanGroupNode = newIndexScanGroup->makeGroupNode(newIndexScan);

  newtopNGroupNode->dependsOn(newProjectGroup);
  newProjectGroupNode->dependsOn(newIndexScanGroup);
  for (auto dep : indexScanGroupNode->dependencies()) {
    newIndexScanGroupNode->dependsOn(dep);
  }

  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newtopNGroupNode);
  return result;
}

std::string PushTopNDownEdgeIndexFullScanRule::toString() const {
  return "PushTopNDownEdgeIndexFullScanRule";
}

}  // namespace opt
}  // namespace nebula
