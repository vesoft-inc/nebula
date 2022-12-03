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
  MatchPathPlanner(CypherClauseContextBase* ctx, const Path& path);

  StatusOr<SubPlan> transform(WhereClauseContext* bindWhereClause,
                              std::unordered_set<std::string> nodeAliasesSeen = {});

 private:
  Status findStarts(WhereClauseContext* bindWhereClause,
                    std::unordered_set<std::string> nodeAliases,
                    bool& startFromEdge,
                    size_t& startIndex,
                    SubPlan& matchClausePlan);

  Status expand(bool startFromEdge, size_t startIndex, SubPlan& subplan);
  Status expandFromNode(size_t startIndex, SubPlan& subplan);
  Status leftExpandFromNode(size_t startIndex, SubPlan& subplan);
  Status rightExpandFromNode(size_t startIndex, SubPlan& subplan);
  Status expandFromEdge(size_t startIndex, SubPlan& subplan);

  void addNodeAlias(const NodeInfo& n) {
    if (!n.anonymous) {
      nodeAliasesSeenInPattern_.emplace(n.alias);
    }
  }

  bool isExpandInto(const std::string& alias) const {
    return nodeAliasesSeenInPattern_.find(alias) != nodeAliasesSeenInPattern_.end();
  }

 private:
  CypherClauseContextBase* ctx_{nullptr};
  Expression* initialExpr_{nullptr};
  const Path& path_;

  // The node alias seen in current pattern only
  std::unordered_set<std::string> nodeAliasesSeenInPattern_;
};
}  // namespace graph
}  // namespace nebula
