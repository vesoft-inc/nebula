/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_YIELDCLAUSEPLANNER_H_
#define GRAPH_PLANNER_MATCH_YIELDCLAUSEPLANNER_H_

#include <vector>  // for vector

#include "common/base/Status.h"                       // for Status
#include "common/base/StatusOr.h"                     // for StatusOr
#include "graph/planner/match/CypherClausePlanner.h"  // for CypherClausePla...

namespace nebula {
class Expression;
class YieldColumns;
namespace graph {
struct CypherClauseContextBase;
struct SubPlan;
struct YieldClauseContext;
}  // namespace graph

class Expression;
class YieldColumns;

namespace graph {
struct CypherClauseContextBase;
struct SubPlan;
struct YieldClauseContext;

/*
 * The YieldClausePlanner was designed to generate plan for yield clause in
 * cypher
 */
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
