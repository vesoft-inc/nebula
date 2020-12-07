/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_MATCHCLAUSEPLANNER_H_
#define PLANNER_MATCH_MATCHCLAUSEPLANNER_H_

#include "planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
/*
 * The MatchClausePlanner was designed to generate plan for match clause;
 */
class MatchClausePlanner final : public CypherClausePlanner {
public:
    MatchClausePlanner() = default;

    StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

private:
    Status findStarts(MatchClauseContext* matchClauseCtx,
                      int64_t& startIndex,
                      SubPlan& matchClausePlan);

    Status expand(const std::vector<NodeInfo>& nodeInfos,
                  const std::vector<EdgeInfo>& edgeInfos,
                  MatchClauseContext* matchClauseCtx,
                  int64_t startIndex,
                  SubPlan& subplan);

    Status appendFetchVertexPlan(const Expression* nodeFilter,
                                 QueryContext* qctx,
                                 SpaceInfo& space,
                                 SubPlan* plan);

    Status projectColumnsBySymbols(MatchClauseContext* matchClauseCtx, SubPlan *plan);

    YieldColumn* buildVertexColumn(const std::string& colName, const std::string& alias) const;

    YieldColumn* buildEdgeColumn(const std::string& colName, EdgeInfo& edge) const;

    YieldColumn* buildPathColumn(const std::string& alias, const PlanNode* input) const;

    Status appendFilterPlan(MatchClauseContext* matchClauseCtx, SubPlan& subplan);

private:
    Expression* initialExpr_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_MATCHCLAUSE_H_
