/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef NGQL_PLANNERS_SUBGRAPHPLANNER_H
#define NGQL_PLANNERS_SUBGRAPHPLANNER_H

#include "graph/context/QueryContext.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

class SubgraphPlanner final : public Planner {
 public:
  using EdgeProp = nebula::storage::cpp2::EdgeProp;
  using Vertexprop = nebula::storage::cpp2::VertexProp;

  static std::unique_ptr<SubgraphPlanner> make() {
    return std::unique_ptr<SubgraphPlanner>(new SubgraphPlanner());
  }

  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kGetSubgraph;
  }

  StatusOr<SubPlan> transform(AstContext* astCtx) override;

  StatusOr<SubPlan> zeroStep(SubPlan& startVidPlan, const std::string& input);

  StatusOr<SubPlan> nSteps(SubPlan& startVidPlan, const std::string& input);

  StatusOr<std::unique_ptr<std::vector<VertexProp>>> buildVertexProps();

  StatusOr<std::unique_ptr<std::vector<EdgeProp>>> buildEdgeProps();

 private:
  SubgraphPlanner() = default;

  SubgraphContext* subgraphCtx_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  //  NGQL_PLANNERS_SUBGRAPHPLANNER_H
