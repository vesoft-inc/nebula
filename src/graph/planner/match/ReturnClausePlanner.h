/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_RETURNCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_RETURNCLAUSEPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
// The ReturnClausePlanner generates plan for return clause.
class ReturnClausePlanner final : public CypherClausePlanner {
 public:
  ReturnClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  Status buildReturn(ReturnClauseContext* rctx, SubPlan& subPlan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_RETURNCLAUSEPLANNER_H_
