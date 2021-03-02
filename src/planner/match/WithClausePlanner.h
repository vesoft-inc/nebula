/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_WITHCLAUSEPLANNER_H_
#define PLANNER_MATCH_WITHCLAUSEPLANNER_H_

#include "planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
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
#endif  // PLANNER_MATCH_WITHCLAUSEPLANNER_H_
