/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_YIELDCLAUSEPLANNER_H_
#define PLANNER_MATCH_YIELDCLAUSEPLANNER_H_

#include "planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
/*
 * The YieldClausePlanner was designed to generate plan for yield clause in cypher
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
#endif  // PLANNER_MATCH_YIELDCLAUSEPLANNER_H_
