/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_

#include "common/base/Status.h"                       // for Status
#include "common/base/StatusOr.h"                     // for StatusOr
#include "graph/planner/match/CypherClausePlanner.h"  // for CypherClausePla...

namespace nebula {
namespace graph {
struct CypherClauseContextBase;
struct OrderByClauseContext;
struct SubPlan;

struct CypherClauseContextBase;
struct OrderByClauseContext;
struct SubPlan;

/*
 * The OrderByClausePlanner was designed to generate plan for order by clause;
 */
class OrderByClausePlanner final : public CypherClausePlanner {
 public:
  OrderByClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  Status buildSort(OrderByClauseContext* octx, SubPlan& subplan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_
