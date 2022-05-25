/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/match/MatchClausePlanner.h"

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/MatchPathPlanner.h"
#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/match/ShortestPathPlanner.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

StatusOr<SubPlan> MatchClausePlanner::transform(CypherClauseContextBase* clauseCtx) {
  if (clauseCtx->kind != CypherClauseKind::kMatch) {
    return Status::Error("Not a valid context for MatchClausePlanner.");
  }

  auto* matchClauseCtx = static_cast<MatchClauseContext*>(clauseCtx);
  SubPlan matchClausePlan;
  // All nodes ever seen in current match clause
  std::unordered_set<std::string> nodeAliasesSeen;
  // TODO: Maybe it is better to rebuild the graph and find all connected components.
  auto& pathInfos = matchClauseCtx->paths;
  for (auto iter = pathInfos.begin(); iter < pathInfos.end(); ++iter) {
    auto& nodeInfos = iter->nodeInfos;
    SubPlan pathPlan;
    if (iter->pathType == Path::PathType::kDefault) {
      MatchPathPlanner matchPathPlanner;
      auto result = matchPathPlanner.transform(matchClauseCtx->qctx,
                                               matchClauseCtx->space.id,
                                               matchClauseCtx->where.get(),
                                               matchClauseCtx->aliasesAvailable,
                                               nodeAliasesSeen,
                                               *iter);
      NG_RETURN_IF_ERROR(result);
      pathPlan = std::move(result).value();
    } else {
      ShortestPathPlanner shortestPathPlanner;
      auto result = shortestPathPlanner.transform(matchClauseCtx->qctx,
                                                  matchClauseCtx->space.id,
                                                  matchClauseCtx->where.get(),
                                                  matchClauseCtx->aliasesAvailable,
                                                  nodeAliasesSeen,
                                                  *iter);
      NG_RETURN_IF_ERROR(result);
      pathPlan = std::move(result).value();
    }
    NG_RETURN_IF_ERROR(
        connectPathPlan(nodeInfos, matchClauseCtx, pathPlan, nodeAliasesSeen, matchClausePlan));
  }
  return matchClausePlan;
}

Status MatchClausePlanner::connectPathPlan(const std::vector<NodeInfo>& nodeInfos,
                                           MatchClauseContext* matchClauseCtx,
                                           const SubPlan& subplan,
                                           std::unordered_set<std::string>& nodeAliasesSeen,
                                           SubPlan& matchClausePlan) {
  std::unordered_set<std::string> intersectedAliases;
  std::for_each(
      nodeInfos.begin(), nodeInfos.end(), [&intersectedAliases, &nodeAliasesSeen](auto& info) {
        if (nodeAliasesSeen.find(info.alias) != nodeAliasesSeen.end()) {
          intersectedAliases.emplace(info.alias);
        }
      });
  std::for_each(nodeInfos.begin(), nodeInfos.end(), [&nodeAliasesSeen](auto& info) {
    if (!info.anonymous) {
      nodeAliasesSeen.emplace(info.alias);
    }
  });
  if (matchClausePlan.root == nullptr) {
    matchClausePlan = subplan;
  } else {
    if (intersectedAliases.empty()) {
      matchClausePlan =
          SegmentsConnector::cartesianProduct(matchClauseCtx->qctx, matchClausePlan, subplan);
    } else {
      // TODO: Actually a natural join would be much easy use.
      matchClausePlan = SegmentsConnector::innerJoin(
          matchClauseCtx->qctx, matchClausePlan, subplan, intersectedAliases);
    }
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
