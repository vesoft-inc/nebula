/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_EXPAND_H_
#define PLANNER_MATCH_EXPAND_H_

#include "common/base/Base.h"
#include "context/ast/QueryAstContext.h"
#include "planner/PlanNode.h"
#include "planner/Planner.h"

namespace nebula {
namespace graph {
/*
 * The Expand was designed to handle the pattern expanding.
 */
class Expand final {
public:
    Expand(MatchClauseContext* matchCtx, Expression** initialExpr)
        : matchCtx_(matchCtx), initialExpr_(initialExpr) {}

    Status doExpand(const NodeInfo& node,
                    const EdgeInfo& edge,
                    const PlanNode* input,
                    SubPlan* plan);

    Status expandSteps(const NodeInfo& node,
                       const EdgeInfo& edge,
                       const PlanNode* input,
                       SubPlan* plan);

    Status expandStep(const EdgeInfo& edge,
                      const PlanNode* input,
                      const Expression* nodeFilter,
                      bool needPassThrough,
                      SubPlan* plan);

    Status collectData(const PlanNode* joinLeft,
                       const PlanNode* joinRight,
                       const PlanNode* inUnionNode,
                       PlanNode** passThrough,
                       SubPlan* plan);

    Status filterDatasetByPathLength(const EdgeInfo& edge, PlanNode* input, SubPlan* plan);

    Expression* initialExprOrEdgeDstExpr(const PlanNode* node);

    PlanNode* joinDataSet(const PlanNode* right, const PlanNode* left);

    template <typename T>
    T* saveObject(T* obj) const {
        return matchCtx_->qctx->objPool()->add(obj);
    }

private:
    std::unique_ptr<std::vector<storage::cpp2::EdgeProp>> genEdgeProps(const EdgeInfo &edge);

    StatusOr<std::vector<storage::cpp2::EdgeProp>> buildAllEdgeProp();

    MatchClauseContext* matchCtx_;
    Expression**        initialExpr_;
};
}   // namespace graph
}   // namespace nebula
#endif  // PLANNER_MATCH_EXPAND_H_
