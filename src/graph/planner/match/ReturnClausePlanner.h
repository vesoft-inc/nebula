/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_MATCH_RETURNCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_RETURNCLAUSEPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
/*
 * The ReturnClausePlanner was designed to generated plan for return clause.
 */
class ReturnClausePlanner final : public CypherClausePlanner {
 public:
  ReturnClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  Status buildReturn(ReturnClauseContext* rctx, SubPlan& subPlan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_RETURNCLAUSEPLANNER_H_
