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
//                 |BiCartesianProduct|
//                 +--------+---------+
//                          |
//                 +--------+---------+
//                 |  ShortestPath    |
//                 +--------+---------+
//                          |
//                 +--------+---------+
//                 |     Project      |
//                 +--------+---------+

StatusOr<SubPlan> ShortestPathPlanner::transform(
    QueryContext* qctx,
    GraphSpaceID spaceId,
    WhereClauseContext* bindWhereClause,
    const std::unordered_map<std::string, AliasType>& aliasesAvailable,
    std::unordered_set<std::string> nodeAliasesSeen,
    Path& path) {
  std::unordered_set<std::string> allNodeAliasesAvailable;
  allNodeAliasesAvailable.merge(nodeAliasesSeen);
  std::for_each(
      aliasesAvailable.begin(), aliasesAvailable.end(), [&allNodeAliasesAvailable](auto& kv) {
        if (kv.second == AliasType::kNode) {
          allNodeAliasesAvailable.emplace(kv.first);
        }
      });

  SubPlan subplan;
  bool singleShortest = path.pathType == Path::PathType::kSingleShortest;
  auto& nodeInfos = path.nodeInfos;
  auto& edge = path.edgeInfos.front();
  std::vector<std::string> colNames;
  colNames.emplace_back(nodeInfos.front().alias);
  colNames.emplace_back(edge.alias);
  colNames.emplace_back(nodeInfos.back().alias);

  auto& startVidFinders = StartVidFinder::finders();
  std::vector<SubPlan> plans;

  for (auto& nodeInfo : nodeInfos) {
    bool foundIndex = false;
    for (auto& finder : startVidFinders) {
      auto nodeCtx = NodeContext(qctx, bindWhereClause, spaceId, &nodeInfo);
      nodeCtx.nodeAliasesAvailable = &allNodeAliasesAvailable;
      auto nodeFinder = finder();
      if (nodeFinder->match(&nodeCtx)) {
        auto status = nodeFinder->transform(&nodeCtx);
        NG_RETURN_IF_ERROR(status);
        auto plan = status.value();
        auto start = StartNode::make(qctx);
        plan.tail->setDep(0, start);
        plan.tail = start;

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

  auto cp = BiCartesianProduct::make(qctx, leftPlan.root, rightPlan.root);

  auto shortestPath = ShortestPath::make(qctx, cp, spaceId, singleShortest);
  auto vertexProp = SchemaUtil::getAllVertexProp(qctx, spaceId, true);
  NG_RETURN_IF_ERROR(vertexProp);
  shortestPath->setVertexProps(std::move(vertexProp).value());
  shortestPath->setEdgeProps(SchemaUtil::getEdgeProps(edge, false, qctx, spaceId));
  shortestPath->setReverseEdgeProps(SchemaUtil::getEdgeProps(edge, true, qctx, spaceId));
  shortestPath->setEdgeDirection(edge.direction);
  shortestPath->setStepRange(edge.range);
  shortestPath->setColNames(std::move(colNames));

  subplan.root = shortestPath;
  subplan.tail = leftPlan.tail;

  MatchSolver::buildProjectColumns(qctx, path, subplan);
  return subplan;
}

}  // namespace graph
}  // namespace nebula
