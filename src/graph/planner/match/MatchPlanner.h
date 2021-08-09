/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCHPLANNER_H_
#define PLANNER_MATCHPLANNER_H_

#include "context/ast/CypherAstContext.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {
class MatchPlanner final : public Planner {
public:
    static std::unique_ptr<MatchPlanner> make() {
        return std::unique_ptr<MatchPlanner>(new MatchPlanner());
    }

    static bool match(AstContext* astCtx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    StatusOr<SubPlan> connectSegments(
        AstContext* astCtx,
        std::vector<SubPlan>& subplans,
        std::vector<std::unique_ptr<CypherClauseContextBase>>& clauses);
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_MATCHPLANNER_H_
