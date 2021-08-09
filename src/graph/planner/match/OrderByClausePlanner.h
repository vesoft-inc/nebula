/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_
#define PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_

#include "planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
/*
 * The OrderByClausePlanner was designed to generate plan for order by clause;
 */
class OrderByClausePlanner final : public CypherClausePlanner {
public:
    OrderByClausePlanner() = default;

    StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

    Status buildSort(OrderByClauseContext* octx, SubPlan& subplan);
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_ORDERBYCLAUSEPLANNER_H_
