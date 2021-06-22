/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_NGQL_GOPLANNER_H_
#define PLANNER_NGQL_GOPLANNER_H_

#include "common/base/Base.h"
#include "context/ast/QueryAstContext.h"
#include "planner/plan/PlanNode.h"
#include "planner/Planner.h"
#include "util/ExpressionUtils.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {
class GoPlanner final : public Planner {
public:
    using EdgeProp = nebula::storage::cpp2::EdgeProp;
    using VertexProp = nebula::storage::cpp2::VertexProp;
    using EdgeProps = std::vector<EdgeProp>;
    using VertexProps = std::vector<VertexProp>;

    static std::unique_ptr<GoPlanner> make() {
        return std::unique_ptr<GoPlanner>(new GoPlanner());
    }

    static bool match(AstContext* astCtx) {
        return astCtx->sentence->kind() == Sentence::Kind::kGo;
    }

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    SubPlan oneStepPlan(SubPlan& startVidPlan);

    SubPlan nStepsPlan(SubPlan& startVidPlan);

    SubPlan mToNStepsPlan(SubPlan& startVidPlan);

private:
    std::unique_ptr<VertexProps> buildVertexProps(const ExpressionProps::TagIDPropsMap& propsMap);

    std::unique_ptr<EdgeProps> buildEdgeProps(bool onlyDst);

    void doBuildEdgeProps(std::unique_ptr<EdgeProps>& edgeProps, bool onlyDst, bool isInEdge);

    Expression* loopCondition(uint32_t steps, const std::string& gnVar);

    PlanNode* extractSrcEdgePropsFromGN(PlanNode* dep, const std::string& input);

    PlanNode* extractSrcDstFromGN(PlanNode* dep, const std::string& input);

    PlanNode* extractVidFromRuntimeInput(PlanNode* dep);

    PlanNode* trackStartVid(PlanNode* left, PlanNode* right);

    PlanNode* buildJoinDstPlan(PlanNode* dep);

    PlanNode* buildJoinInputPlan(PlanNode* dep);

    PlanNode* lastStepJoinInput(PlanNode* left, PlanNode* right);

    PlanNode* buildLastStepJoinPlan(PlanNode* gn, PlanNode* join);

    PlanNode* lastStep(PlanNode* dep, PlanNode* join);

    PlanNode* buildOneStepJoinPlan(PlanNode* gn);

private:
    GoPlanner() = default;

    GoContext* goCtx_{nullptr};

    const int16_t VID_INDEX = 0;
    const int16_t LAST_COL_INDEX = -1;
};
}  // namespace graph
}  // namespace nebula

#endif  // PLANNER_NGQL_GOPLANNER_H
