// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#pragma once

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
// The MatchPathPlanner generates plan for match clause;
class MatchPathPlanner final {
 public:
  MatchPathPlanner() = default;

  StatusOr<SubPlan> transform(QueryContext* qctx,
                              GraphSpaceID spaceId,
                              WhereClauseContext* bindWhereClause,
                              const std::unordered_map<std::string, AliasType>& aliasesAvailable,
                              std::unordered_set<std::string> nodeAliasesSeen,
                              Path& path);

 private:
  Status findStarts(std::vector<NodeInfo>& nodeInfos,
                    std::vector<EdgeInfo>& edgeInfos,
                    QueryContext* qctx,
                    GraphSpaceID spaceId,
                    WhereClauseContext* bindWhereClause,
                    const std::unordered_map<std::string, AliasType>& aliasesAvailable,
                    std::unordered_set<std::string> nodeAliases,
                    bool& startFromEdge,
                    size_t& startIndex,
                    SubPlan& matchClausePlan);

  Status expand(const std::vector<NodeInfo>& nodeInfos,
                const std::vector<EdgeInfo>& edgeInfos,
                QueryContext* qctx,
                GraphSpaceID spaceId,
                bool startFromEdge,
                size_t startIndex,
                SubPlan& subplan,
                std::unordered_set<std::string>& nodeAliasesSeenInPattern);

  Status expandFromNode(const std::vector<NodeInfo>& nodeInfos,
                        const std::vector<EdgeInfo>& edgeInfos,
                        QueryContext* qctx,
                        GraphSpaceID spaceId,
                        size_t startIndex,
                        SubPlan& subplan,
                        std::unordered_set<std::string>& nodeAliasesSeenInPattern);

  Status leftExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                            const std::vector<EdgeInfo>& edgeInfos,
                            QueryContext* qctx,
                            GraphSpaceID spaceId,
                            size_t startIndex,
                            std::string inputVar,
                            SubPlan& subplan,
                            std::unordered_set<std::string>& nodeAliasesSeenInPattern);

  Status rightExpandFromNode(const std::vector<NodeInfo>& nodeInfos,
                             const std::vector<EdgeInfo>& edgeInfos,
                             QueryContext* qctx,
                             GraphSpaceID spaceId,
                             size_t startIndex,
                             SubPlan& subplan,
                             std::unordered_set<std::string>& nodeAliasesSeenInPattern);

  Status expandFromEdge(const std::vector<NodeInfo>& nodeInfos,
                        const std::vector<EdgeInfo>& edgeInfos,
                        QueryContext* qctx,
                        GraphSpaceID spaceId,
                        size_t startIndex,
                        SubPlan& subplan,
                        std::unordered_set<std::string>& nodeAliasesSeenInPattern);

 private:
  Expression* initialExpr_{nullptr};
};
}  // namespace graph
}  // namespace nebula
