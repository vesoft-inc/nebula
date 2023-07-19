/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_MATCHCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_MATCHCLAUSEPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
// The MatchClausePlanner generates plan for match clause;
class MatchClausePlanner final : public CypherClausePlanner {
 public:
  MatchClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

 private:
  Status connectPathPlan(const std::vector<NodeInfo>& nodeInfos,
                         MatchClauseContext* matchClauseCtx,
                         const SubPlan& subplan,
                         std::unordered_set<std::string>& nodeAliasesSeen,
                         SubPlan& matchClausePlan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_MATCHCLAUSE_H_
