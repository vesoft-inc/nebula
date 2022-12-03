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
  ShortestPathPlanner(CypherClauseContextBase* ctx, const Path& path);

  StatusOr<SubPlan> transform(WhereClauseContext* bindWhereClause,
                              std::unordered_set<std::string> nodeAliasesSeen = {});

 private:
  CypherClauseContextBase* ctx_{nullptr};
  const Path& path_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_SHORTESTPATHPLANNER_H_
