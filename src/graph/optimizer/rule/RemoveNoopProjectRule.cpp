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

/*static*/
const std::unordered_set<graph::PlanNode::Kind> RemoveNoopProjectRule::kQueries{
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
    PlanNode::Kind::kInnerJoin,
    PlanNode::Kind::kHashLeftJoin,
    PlanNode::Kind::kHashInnerJoin,
    PlanNode::Kind::kCrossJoin,
    PlanNode::Kind::kRollUpApply,
    PlanNode::Kind::kPatternApply,
    PlanNode::Kind::kArgument,
};

std::unique_ptr<OptRule> RemoveNoopProjectRule::kInstance =
    std::unique_ptr<RemoveNoopProjectRule>(new RemoveNoopProjectRule());

RemoveNoopProjectRule::RemoveNoopProjectRule() {
  RuleSet::QueryRules().addRule(this);
}

const Pattern& RemoveNoopProjectRule::pattern() const {
  static Pattern pattern =
      Pattern::create(graph::PlanNode::Kind::kProject, {Pattern::create(PlanNode::Kind::kUnknown)});
  return pattern;
}

StatusOr<OptRule::TransformResult> RemoveNoopProjectRule::transform(
    OptContext* octx, const MatchedResult& matched) const {
  const auto* oldProjNode = matched.planNode({0});
  DCHECK_EQ(oldProjNode->kind(), PlanNode::Kind::kProject);
  auto* projGroup = const_cast<OptGroup*>(matched.node->group());

  const auto* depPlanNode = matched.planNode({0, 0});
  auto* newNode = depPlanNode->clone();
  newNode->setOutputVar(oldProjNode->outputVar());
  newNode->setColNames(oldProjNode->colNames());
  newNode->setInputVar(depPlanNode->inputVar());
  auto* newGroupNode = OptGroupNode::create(octx, newNode, projGroup);
  newGroupNode->setDeps(matched.result({0, 0}).node->dependencies());

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

  const auto* depNode = matched.planNode({0, 0});
  if (kQueries.find(depNode->kind()) == kQueries.end()) {
    return false;
  }

  const auto& depColNames = depNode->colNames();
  const auto& projColNames = projNode->colNames();
  auto numCols = depColNames.size();
  if (numCols != projColNames.size()) {
    return false;
  }

  std::vector<YieldColumn*> cols = projNode->columns()->columns();
  for (size_t i = 0; i < numCols; ++i) {
    const auto* expr = DCHECK_NOTNULL(cols[i]->expr());
    if (expr->kind() != Expression::Kind::kVarProperty &&
        expr->kind() != Expression::Kind::kInputProperty) {
      return false;
    }

    const auto* propExpr = static_cast<PropertyExpression*>(cols[i]->expr());
    if (propExpr->prop() != projColNames[i] || depColNames[i].compare(projColNames[i])) {
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
