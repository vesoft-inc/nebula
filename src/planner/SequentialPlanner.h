/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_SEQUENTIALPLANNER_H_
#define PLANNER_PLANNERS_SEQUENTIALPLANNER_H_

#include "context/QueryContext.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {
class SequentialPlanner final : public Planner {
public:
    static std::unique_ptr<SequentialPlanner> make() {
        return std::unique_ptr<SequentialPlanner>(new SequentialPlanner());
    }

    static bool match(AstContext* astCtx);

    /**
     * Each sentence would be converted to a sub-plan, and they would
     * be cascaded together into a complete execution plan.
     */
    StatusOr<SubPlan> transform(AstContext* astCtx) override;

    void ifBuildDataCollect(SubPlan& subPlan, QueryContext* qctx);

private:
    SequentialPlanner() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLANNERS_SEQUENTIALPLANNER_H_
