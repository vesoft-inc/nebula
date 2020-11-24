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

struct AstContext;

class MatchSolver final {
public:
    MatchSolver() = delete;
    ~MatchSolver() = delete;

    static Status buildReturn(MatchAstContext* matchCtx, SubPlan& subPlan);

    static Expression* rewrite(const LabelExpression *label);

    static Expression* rewrite(const LabelAttributeExpression *la);

    static Expression* doRewrite(const MatchAstContext* mctx, const Expression* expr);

    static bool match(AstContext* astCtx);

    static Expression* makeIndexFilter(const std::string& label,
                                       const MapExpression* map,
                                       QueryContext* qctx);

    static Expression* makeIndexFilter(const std::string& label,
                                       const std::string& alias,
                                       Expression* filter,
                                       QueryContext* qctx);

    static Status buildFilter(const MatchAstContext* mctx, SubPlan* plan);
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_MATCHSOLVER_H_
