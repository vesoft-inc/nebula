/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
// The OrderByClausePlanner generates plan for order by clause;
class OrderByClausePlanner final : public CypherClausePlanner {
 public:
  OrderByClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  Status buildSort(OrderByClauseContext* octx, SubPlan& subplan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_
