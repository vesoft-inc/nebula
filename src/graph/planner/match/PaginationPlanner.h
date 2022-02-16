/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_PAGINATIONPLANNER_H_
#define GRAPH_PLANNER_MATCH_PAGINATIONPLANNER_H_

#include "common/base/Status.h"                       // for Status
#include "common/base/StatusOr.h"                     // for StatusOr
#include "graph/planner/match/CypherClausePlanner.h"  // for CypherClausePla...

namespace nebula {
namespace graph {
struct CypherClauseContextBase;
struct PaginationContext;
struct SubPlan;

struct CypherClauseContextBase;
struct PaginationContext;
struct SubPlan;

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
