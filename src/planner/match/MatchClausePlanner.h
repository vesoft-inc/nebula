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
                      bool& startFromEdge,
                      size_t& startIndex,
                      SubPlan& matchClausePlan);

    Status expand(const std::vector<NodeInfo>& nodeInfos,
                  const std::vector<EdgeInfo>& edgeInfos,
                  MatchClauseContext* matchClauseCtx,
                  bool startFromEdge,
                  size_t startIndex,
                  SubPlan& subplan);

    Status expandFromNode(const std::vector<NodeInfo>& nodeInfos,
                          const std::vector<EdgeInfo>& edgeInfos,
                          MatchClauseContext* matchClauseCtx,
                          size_t startIndex,
                          SubPlan& subplan);

    PlanNode* joinLeftAndRightExpandPart(QueryContext* qctx, PlanNode* left, PlanNode* right);

    Status leftExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                              const std::vector<EdgeInfo>& edgeInfos,
                              MatchClauseContext* matchClauseCtx,
                              size_t startIndex,
                              std::string inputVar,
                              SubPlan& subplan);

    Status rightExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                               const std::vector<EdgeInfo>& edgeInfos,
                               MatchClauseContext* matchClauseCtx,
                               size_t startIndex,
                               SubPlan& subplan);

    Status expandFromEdge(const std::vector<NodeInfo>& nodeInfos,
                          const std::vector<EdgeInfo>& edgeInfos,
                          MatchClauseContext* matchClauseCtx,
                          size_t startIndex,
                          SubPlan& subplan);

    Status appendFetchVertexPlan(const Expression* nodeFilter,
                                 const SpaceInfo& space,
                                 QueryContext* qctx,
                                 Expression* initialExpr,
                                 SubPlan& plan);

    Status projectColumnsBySymbols(MatchClauseContext* matchClauseCtx,
                                   size_t startIndex,
                                   SubPlan& plan);

    YieldColumn* buildVertexColumn(const std::string& colName, const std::string& alias) const;

    YieldColumn* buildEdgeColumn(const std::string& colName, EdgeInfo& edge) const;

    YieldColumn* buildPathColumn(const std::string& alias,
                                 size_t startIndex,
                                 const std::vector<std::string> colNames,
                                 size_t nodeInfoSize) const;

    Status appendFilterPlan(MatchClauseContext* matchClauseCtx, SubPlan& subplan);
    // Fetch all tags in the space and retrieve props from tags
    StatusOr<std::vector<storage::cpp2::VertexProp>> flattenTags(QueryContext *qctx,
                                                                 const SpaceInfo& space);

private:
    std::unique_ptr<Expression> initialExpr_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_MATCHCLAUSE_H_
