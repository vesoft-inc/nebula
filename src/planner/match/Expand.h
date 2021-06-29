/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_EXPAND_H_
#define PLANNER_MATCH_EXPAND_H_

#include "common/base/Base.h"
#include "context/ast/CypherAstContext.h"
#include "planner/plan/PlanNode.h"
#include "planner/Planner.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
/*
 * The Expand was designed to handle the pattern expanding.
 */
class Expand final {
public:
    Expand(MatchClauseContext* matchCtx, Expression* initialExpr)
        : matchCtx_(matchCtx), initialExpr_(initialExpr) {}

    Expand* reversely() {
        reversely_ = true;
        return this;
    }

    Expand* depends(PlanNode* dep) {
        dependency_ = dep;
        return this;
    }

    Expand* inputVar(const std::string& inputVar) {
        inputVar_ = inputVar;
        return this;
    }

    Status doExpand(const NodeInfo& node,
                    const EdgeInfo& edge,
                    SubPlan* plan);

private:
    Status expandSteps(const NodeInfo& node,
                       const EdgeInfo& edge,
                       SubPlan* plan);

    Status expandStep(const EdgeInfo& edge,
                      PlanNode* dep,
                      const std::string& inputVar,
                      const Expression* nodeFilter,
                      SubPlan* plan);

    Status collectData(const PlanNode* joinLeft,
                       const PlanNode* joinRight,
                       PlanNode** passThrough,
                       SubPlan* plan);

    Status filterDatasetByPathLength(const EdgeInfo& edge,
                                     PlanNode* input,
                                     SubPlan* plan);

    Expression* buildExpandCondition(const std::string& lastStepResult,
                                     int64_t startIndex,
                                     int64_t maxHop) const;

    template <typename T>
    T* saveObject(T* obj) const {
        return matchCtx_->qctx->objPool()->add(obj);
    }

    std::unique_ptr<std::vector<storage::cpp2::EdgeProp>> genEdgeProps(const EdgeInfo &edge);

    MatchClauseContext*                 matchCtx_;
    Expression*                         initialExpr_{nullptr};
    bool                                reversely_{false};
    PlanNode*                           dependency_{nullptr};
    std::string                         inputVar_;
};
}   // namespace graph
}   // namespace nebula
#endif  // PLANNER_MATCH_EXPAND_H_
