/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCHSOLVER_H_
#define PLANNER_PLANNERS_MATCHSOLVER_H_

#include "planner/Planner.h"
#include "validator/MatchValidator.h"

namespace nebula {
namespace graph {
class MatchSolver final {
public:
    MatchSolver() = delete;
    ~MatchSolver() = delete;

    static Status buildReturn(MatchAstContext* matchCtx, SubPlan& subPlan);

    static Expression* rewrite(const LabelExpression *label);

    static Expression* rewrite(const LabelAttributeExpression *la);
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_MATCHSOLVER_H_
