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
  PlanNode* joinLeftAndRightExpandPart(QueryContext* qctx, PlanNode* left, PlanNode* right);

  Status appendFilterPlan(MatchClauseContext* matchClauseCtx, SubPlan& subplan);

  Status connectPathPlan(const std::vector<NodeInfo>& nodeInfos,
                         MatchClauseContext* matchClauseCtx,
                         const SubPlan& subplan,
                         std::unordered_set<std::string>& nodeAliasesSeen,
                         SubPlan& matchClausePlan);

  Status buildShortestPath(const std::vector<NodeInfo>& nodeInfos,
                           std::vector<EdgeInfo>& edgeInfos,
                           MatchClauseContext* matchClauseCtx,
                           SubPlan& subplan,
                           bool single);

  StatusOr<std::vector<IndexID>> pickTagIndex(MatchClauseContext* matchClauseCtx,
                                              NodeInfo nodeInfo);

 private:
  Expression* initialExpr_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_MATCHCLAUSE_H_
