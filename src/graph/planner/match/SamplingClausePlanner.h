/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_

#include "graph/planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
// The SamplingClausePlanner generates plan for order by clause;
class SamplingClausePlanner final : public CypherClausePlanner {
 public:
  SamplingClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  Status buildSampling(SamplingClauseContext* octx, SubPlan& subplan);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_
