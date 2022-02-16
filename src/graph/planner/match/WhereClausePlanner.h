/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_WHERECLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_WHERECLAUSEPLANNER_H_

#include "common/base/StatusOr.h"                     // for StatusOr
#include "graph/planner/match/CypherClausePlanner.h"  // for CypherClausePla...

namespace nebula {
namespace graph {
struct CypherClauseContextBase;
struct SubPlan;

struct CypherClauseContextBase;
struct SubPlan;

/*
 * The WhereClausePlanner was designed to generate plan for where clause.
 */
class WhereClausePlanner final : public CypherClausePlanner {
 public:
  explicit WhereClausePlanner(bool needStableFilter = false)
      : needStableFilter_(needStableFilter) {}

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

 private:
  // `needStableFilter_=true` only if there is orderBy in withClause(to avoid
  // unstableErase)
  bool needStableFilter_{false};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_WHERECLAUSEPLANNER_H_
