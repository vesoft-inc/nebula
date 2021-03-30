/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_WHERECLAUSEPLANNER_H_
#define PLANNER_MATCH_WHERECLAUSEPLANNER_H_

#include "planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
/*
 * The WhereClausePlanner was designed to generate plan for where clause.
 */
class WhereClausePlanner final : public CypherClausePlanner {
public:
    WhereClausePlanner() = default;

    StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;
};
}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_MATCH_WHERECLAUSEPLANNER_H_
