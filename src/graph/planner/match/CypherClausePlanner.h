/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_CYPHERCLAUSEPLANNER_H_
#define PLANNER_MATCH_CYPHERCLAUSEPLANNER_H_

#include "context/ast/CypherAstContext.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {
class CypherClausePlanner {
public:
    CypherClausePlanner() = default;
    virtual ~CypherClausePlanner() = default;

    virtual StatusOr<SubPlan> transform(CypherClauseContextBase* clauseCtx) = 0;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_CYPHERCLAUSEPLANNER_H_
