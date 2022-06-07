// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_PLANNER_MATCH_SHORTESTPATHPLANNER_H_
#define GRAPH_PLANNER_MATCH_SHORTESTPATHPLANNER_H_

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"

namespace nebula {
namespace graph {
class ShortestPathPlanner final {
 public:
  ShortestPathPlanner() = default;

  StatusOr<SubPlan> transform(QueryContext* qctx,
                              GraphSpaceID spaceId,
                              WhereClauseContext* bindWhereClause,
                              const std::unordered_map<std::string, AliasType>& aliasesAvailable,
                              std::unordered_set<std::string> nodeAliasesSeen,
                              Path& path);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_SHORTESTPATHPLANNER_H_
