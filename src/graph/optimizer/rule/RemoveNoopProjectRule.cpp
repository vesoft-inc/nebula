/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/RemoveNoopProjectRule.h"

#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

using nebula::graph::PlanNode;
using nebula::graph::QueryContext;

namespace nebula {
namespace opt {

/*static*/ const std::initializer_list<graph::PlanNode::Kind> RemoveNoopProjectRule::kQueries{
    PlanNode::Kind::kGetNeighbors,
    PlanNode::Kind::kGetVertices,
    PlanNode::Kind::kGetEdges,
    PlanNode::Kind::kTraverse,
    PlanNode::Kind::kAppendVertices,

    // ------------------
    // TODO(yee): refactor in logical plan
    PlanNode::Kind::kIndexScan,
    PlanNode::Kind::kTagIndexFullScan,
    PlanNode::Kind::kTagIndexPrefixScan,
    PlanNode::Kind::kTagIndexRangeScan,
    PlanNode::Kind::kEdgeIndexFullScan,
    PlanNode::Kind::kEdgeIndexPrefixScan,
    PlanNode::Kind::kEdgeIndexRangeScan,
    PlanNode::Kind::kScanVertices,
    PlanNode::Kind::kScanEdges,
    // ------------------
    // PlanNode::Kind::kFilter,  filter has no value in result
    PlanNode::Kind::kUnion,
    PlanNode::Kind::kUnionAllVersionVar,
    // PlanNode::Kind::kIntersect, intersect has no value in result
    // PlanNode::Kind::kMinus, minus has no value in result
    PlanNode::Kind::kProject,
    PlanNode::Kind::kUnwind,
    PlanNode::Kind::kSort,
    PlanNode::Kind::kTopN,
    // PlanNode::Kind::kLimit, limit has no value in result
    PlanNode::Kind::kSample,
    PlanNode::Kind::kAggregate,
    // PlanNode::Kind::kDedup, dedup has no value in result
    PlanNode::Kind::kAssign,
    PlanNode::Kind::kBFSShortest,
    PlanNode::Kind::kMultiShortestPath,
    PlanNode::Kind::kProduceAllPaths,
    PlanNode::Kind::kCartesianProduct,
    PlanNode::Kind::kSubgraph,
    PlanNode::Kind::kDataCollect,
    PlanNode::Kind::kLeftJoin,
    PlanNode::Kind::kInnerJoin,
    PlanNode::Kind::kBiLeftJoin,
    PlanNode::Kind::kBiInnerJoin,
    PlanNode::Kind::kBiCartesianProduct,
    PlanNode::Kind::kRollUpApply,
    PlanNode::Kind::kArgument};

std::unique_ptr<OptRule> RemoveNoopProjectRule::kInstance =
    std::unique_ptr<RemoveNoopProjectRule>(new RemoveNoopProjectRule());

RemoveNoopProjectRule::RemoveNoopProjectRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& RemoveNoopProjectRule::pattern() const {
  static Pattern pattern =
      Pattern::create(graph::PlanNode::Kind::kProject, {Pattern::create(kQueries)});
  return pattern;
}

StatusOr<OptRule::TransformResult> RemoveNoopProjectRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  const auto* projGroupNode = matched.node;
  const auto* oldProjNode = projGroupNode->node();
  const auto* projGroup = projGroupNode->group();
  DCHECK_EQ(oldProjNode->kind(), PlanNode::Kind::kProject);
  const auto* depGroupNode = matched.dependencies.front().node;
  auto* newNode = depGroupNode->node()->clone();
  newNode->setOutputVar(oldProjNode->outputVar());
  auto* newGroupNode = OptGroupNode::create(octx, newNode, projGroup);
  newGroupNode->setDeps(depGroupNode->dependencies());
  TransformResult result;
  result.eraseAll = true;
  result.newGroupNodes.emplace_back(newGroupNode);

  return result;
}

bool RemoveNoopProjectRule::match(OptContext* octx, const MatchedResult& matched) const {
  if (!OptRule::match(octx, matched)) {
    return false;
  }

  auto* projGroupNode = matched.node;
  DCHECK_EQ(projGroupNode->node()->kind(), PlanNode::Kind::kProject);

  auto* projNode = static_cast<const graph::Project*>(projGroupNode->node());
  std::vector<YieldColumn*> cols = projNode->columns()->columns();
  for (auto* col : cols) {
    if (col->expr()->kind() != Expression::Kind::kVarProperty &&
        col->expr()->kind() != Expression::Kind::kInputProperty) {
      return false;
    }
  }
  const auto* depGroupNode = matched.dependencies.front().node;
  const auto* depNode = depGroupNode->node();
  const auto& depColNames = depNode->colNames();
  const auto& projColNames = projNode->colNames();
  auto colsNum = depColNames.size();
  if (colsNum != projColNames.size()) {
    return false;
  }
  for (size_t i = 0; i < colsNum; ++i) {
    if (depColNames[i].compare(projColNames[i])) {
      return false;
    }
    const auto* propExpr = static_cast<PropertyExpression*>(cols[i]->expr());
    if (propExpr->prop() != projColNames[i]) {
      return false;
    }
  }

  return true;
}

std::string RemoveNoopProjectRule::toString() const {
  return "RemoveNoopProjectRule";
}

}  // namespace opt
}  // namespace nebula
