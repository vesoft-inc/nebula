/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_WITHCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_WITHCLAUSEPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
// The WithClausePlanner generates plan for with clause.
class WithClausePlanner final : public CypherClausePlanner {
 public:
  WithClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

 private:
  Status buildWith(WithClauseContext* wctx, SubPlan& subPlan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_WITHCLAUSEPLANNER_H_
