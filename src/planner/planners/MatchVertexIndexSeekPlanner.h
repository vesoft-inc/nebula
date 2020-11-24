/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCHVERTEXINDEXSEEKPLANNER_H_
#define PLANNER_PLANNERS_MATCHVERTEXINDEXSEEKPLANNER_H_

#include "common/expression/LabelExpression.h"
#include "context/QueryContext.h"
#include "planner/Planner.h"
#include "validator/MatchValidator.h"

namespace nebula {
namespace graph {
class MatchVertexIndexSeekPlanner final : public Planner {
public:
    static std::unique_ptr<MatchVertexIndexSeekPlanner> make() {
        return std::unique_ptr<MatchVertexIndexSeekPlanner>(new MatchVertexIndexSeekPlanner());
    }

    static bool match(AstContext* astCtx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    using VertexProp = nebula::storage::cpp2::VertexProp;
    using EdgeProp = nebula::storage::cpp2::EdgeProp;

    MatchVertexIndexSeekPlanner() = default;

    Status buildScanNode();

    Status buildSteps();

    Status buildStep();

    Status buildGetTailVertices();

    Status buildStepJoin();

    Status buildTailJoin();

    template <typename T>
    T* saveObject(T *obj) const {
        return matchCtx_->qctx->objPool()->add(obj);
    }

    SubPlan                                     subPlan_;
    MatchAstContext                            *matchCtx_;
    bool                                        startFromNode_{true};
    int32_t                                     startIndex_{0};
    int32_t                                     curStep_{-1};
    PlanNode                                   *thisStepRoot_{nullptr};
    PlanNode                                   *prevStepRoot_{nullptr};
    Expression                                 *gnSrcExpr_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif
