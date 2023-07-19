/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_YIELDCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_YIELDCLAUSEPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
// The YieldClausePlanner generates plan for yield clause
class YieldClausePlanner final : public CypherClausePlanner {
 public:
  YieldClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  void rewriteYieldColumns(const YieldClauseContext* yctx,
                           const YieldColumns* yields,
                           YieldColumns* newYields);

  void rewriteGroupExprs(const YieldClauseContext* yctx,
                         const std::vector<Expression*>* exprs,
                         std::vector<Expression*>* newExprs);

  Status buildYield(YieldClauseContext* yctx, SubPlan& subplan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_YIELDCLAUSEPLANNER_H_
