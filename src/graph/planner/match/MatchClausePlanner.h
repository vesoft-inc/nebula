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
  Status findStarts(std::vector<NodeInfo>& nodeInfos,
                    std::vector<EdgeInfo>& edgeInfos,
                    MatchClauseContext* matchClauseCtx,
                    std::unordered_set<std::string> nodeAliases,
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

  // Project all named alias.
  // TODO: Might not neccessary
  Status projectColumnsBySymbols(MatchClauseContext* matchClauseCtx, SubPlan& plan);

  YieldColumn* buildVertexColumn(MatchClauseContext* matchClauseCtx,
                                 const std::string& alias) const;

  YieldColumn* buildEdgeColumn(MatchClauseContext* matchClauseCtx, EdgeInfo& edge) const;

  YieldColumn* buildPathColumn(Expression* pathBuild, const std::string& alias) const;

  Status appendFilterPlan(MatchClauseContext* matchClauseCtx, SubPlan& subplan);

  Status connectPathPlan(const std::vector<NodeInfo>& nodeInfos,
                         MatchClauseContext* matchClauseCtx,
                         const SubPlan& subplan,
                         std::unordered_set<std::string>& nodeAliasesSeen,
                         SubPlan& matchClausePlan);

 private:
  Expression* initialExpr_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_MATCHCLAUSE_H_
