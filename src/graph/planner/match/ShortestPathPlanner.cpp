// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/planner/match/ShortestPathPlanner.h"

#include "graph/planner/match/MatchSolver.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/match/StartVidFinder.h"
#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/SchemaUtil.h"

namespace nebula {
namespace graph {

ShortestPathPlanner::ShortestPathPlanner(CypherClauseContextBase* ctx, const Path& path)
    : ctx_(DCHECK_NOTNULL(ctx)), path_(path) {}

//              The plan looks like this:
//    +--------+---------+        +---------+--------+
//    |      Start       |        |       Start      |
//    +--------+---------+        +---------+--------+
//             |                            |
//    +--------+---------+        +---------+--------+
//    |indexScan/Argument|        |indexScan/Argument|
//    +--------+---------+        +---------+--------+
//             |                            |
//    +--------+---------+        +---------+--------+
//    |    Project       |        |     Project      |
//    +--------+---------+        +---------+--------+
//             |                            |
//             +------------+---------------+
//                          |
//                 +--------+---------+
//                 |    CrossJoin     |
//                 +--------+---------+
//                          |
//                 +--------+---------+
//                 |  ShortestPath    |
//                 +--------+---------+
//                          |
//                 +--------+---------+
//                 |     Project      |
//                 +--------+---------+
StatusOr<SubPlan> ShortestPathPlanner::transform(WhereClauseContext* bindWhereClause,
                                                 std::unordered_set<std::string> nodeAliasesSeen) {
  for (auto& kv : ctx_->aliasesAvailable) {
    if (kv.second == AliasType::kNode) {
      nodeAliasesSeen.emplace(kv.first);
    }
  }

  SubPlan subplan;
  bool singleShortest = path_.pathType == Path::PathType::kSingleShortest;
  auto& nodeInfos = path_.nodeInfos;
  auto& edge = path_.edgeInfos.front();
  std::vector<std::string> colNames;
  colNames.emplace_back(nodeInfos.front().alias);
  colNames.emplace_back(edge.alias);
  colNames.emplace_back(nodeInfos.back().alias);

  auto& startVidFinders = StartVidFinder::finders();
  std::vector<SubPlan> plans;

  auto qctx = ctx_->qctx;
  auto spaceId = ctx_->space.id;
  for (auto& nodeInfo : nodeInfos) {
    bool foundIndex = false;
    for (auto& finder : startVidFinders) {
      auto nodeCtx = NodeContext(qctx, bindWhereClause, spaceId, &nodeInfo);
      nodeCtx.aliasesAvailable = &nodeAliasesSeen;
      auto nodeFinder = finder();
      if (nodeFinder->match(&nodeCtx)) {
        auto status = nodeFinder->transform(&nodeCtx);
        NG_RETURN_IF_ERROR(status);
        auto plan = std::move(status).value();
        plan.appendStartNode(qctx);

        auto initExpr = nodeCtx.initialExpr->clone();
        auto columns = qctx->objPool()->makeAndAdd<YieldColumns>();
        columns->addColumn(new YieldColumn(initExpr, nodeInfo.alias));
        plan.root = Project::make(qctx, plan.root, columns);

        plans.emplace_back(std::move(plan));
        foundIndex = true;
        break;
      }
    }
    if (!foundIndex) {
      return Status::SemanticError("Can't find index from shortestPath pattern");
    }
  }
  auto& leftPlan = plans.front();
  auto& rightPlan = plans.back();

  auto cp = CrossJoin::make(qctx, leftPlan.root, rightPlan.root);

  MatchStepRange stepRange(1, 1);
  if (edge.range != nullptr) {
    stepRange = *edge.range;
  }

  auto shortestPath = ShortestPath::make(qctx, cp, spaceId, singleShortest);
  auto vertexProp = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProp);
  shortestPath->setVertexProps(std::move(vertexProp).value());
  shortestPath->setEdgeProps(SchemaUtil::getEdgeProps(edge, false, qctx, spaceId));
  shortestPath->setReverseEdgeProps(SchemaUtil::getEdgeProps(edge, true, qctx, spaceId));
  shortestPath->setEdgeDirection(edge.direction);
  shortestPath->setStepRange(stepRange);
  shortestPath->setColNames(std::move(colNames));

  subplan.root = shortestPath;
  subplan.tail = leftPlan.tail;

  MatchSolver::buildProjectColumns(qctx, path_, subplan);
  return subplan;
}

}  // namespace graph
}  // namespace nebula
