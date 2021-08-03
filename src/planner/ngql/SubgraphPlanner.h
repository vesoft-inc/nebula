/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NGQL_PLANNERS_SUBGRAPHPLANNER_H
#define NGQL_PLANNERS_SUBGRAPHPLANNER_H

#include "context/QueryContext.h"
#include "context/ast/QueryAstContext.h"
#include "planner/Planner.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

class SubgraphPlanner final : public Planner {
public:
    using EdgeProp = nebula::storage::cpp2::EdgeProp;

    static std::unique_ptr<SubgraphPlanner> make() {
        return std::unique_ptr<SubgraphPlanner>(new SubgraphPlanner());
    }

    static bool match(AstContext* astCtx) {
        return astCtx->sentence->kind() == Sentence::Kind::kGetSubgraph;
    }

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

    StatusOr<SubPlan> zeroStep(SubPlan& startVidPlan, const std::string& input);

    StatusOr<SubPlan> nSteps(SubPlan& startVidPlan, const std::string& input);

    StatusOr<std::unique_ptr<std::vector<EdgeProp>>> buildEdgeProps();

    Expression* loopCondition(uint32_t steps, const std::string& var);

private:
    SubgraphPlanner() = default;

    SubgraphContext* subgraphCtx_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  //  NGQL_PLANNERS_SUBGRAPHPLANNER_H
