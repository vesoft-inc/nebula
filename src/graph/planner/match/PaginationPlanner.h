/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_MATCH_PAGINATIONPLANNER_H_
#define GRAPH_PLANNER_MATCH_PAGINATIONPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
/*
 * The PaginationPlanner was designed to generate subplan for skip/limit clause.
 */
class PaginationPlanner final : public CypherClausePlanner {
 public:
  PaginationPlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  Status buildLimit(PaginationContext* pctx, SubPlan& subplan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_PAGINATIONPLANNER_H_
