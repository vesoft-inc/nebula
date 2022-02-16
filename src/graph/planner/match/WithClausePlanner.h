/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_WITHCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_WITHCLAUSEPLANNER_H_

#include "common/base/Status.h"                       // for Status
#include "common/base/StatusOr.h"                     // for StatusOr
#include "graph/planner/match/CypherClausePlanner.h"  // for CypherClausePla...

namespace nebula {
namespace graph {
struct CypherClauseContextBase;
struct SubPlan;
struct WithClauseContext;

struct CypherClauseContextBase;
struct SubPlan;
struct WithClauseContext;

/*
 * The WithClausePlanner was designed to generate plan for with clause.
 */
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
