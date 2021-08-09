/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_UNWINDCLAUSEPLANNER_H_
#define PLANNER_MATCH_UNWINDCLAUSEPLANNER_H_

#include "planner/match/CypherClausePlanner.h"

namespace nebula {
namespace graph {
/*
 * The UnwindClausePlanner was designed to generate plan for unwind clause
 */
class UnwindClausePlanner final : public CypherClausePlanner {
public:
    UnwindClausePlanner() = default;

    StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) override;

    Status buildUnwind(UnwindClauseContext* uctx, SubPlan& subPlan);
};

}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_MATCH_UNWINDCLAUSEPLANNER_H_
