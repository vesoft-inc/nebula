/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_UNWINDCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_UNWINDCLAUSEPLANNER_H_

#include "common/base/Status.h"                       // for Status
#include "common/base/StatusOr.h"                     // for StatusOr
#include "graph/planner/match/CypherClausePlanner.h"  // for CypherClausePla...

namespace nebula {
namespace graph {
struct CypherClauseContextBase;
struct SubPlan;
struct UnwindClauseContext;

struct CypherClauseContextBase;
struct SubPlan;
struct UnwindClauseContext;

/*
 * The UnwindClausePlanner was designed to generate plan for unwind clause
 */
class UnwindClausePlanner final : public CypherClausePlanner {
 public:
  UnwindClausePlanner() = default;

  StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

  Status buildUnwind(UnwindClauseContext* uctx, SubPlan& subPlan);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_UNWINDCLAUSEPLANNER_H_
