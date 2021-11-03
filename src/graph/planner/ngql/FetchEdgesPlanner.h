/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_NGQL_FETCH_EDGES_H_
#define GRAPH_PLANNER_NGQL_FETCH_EDGES_H_

#include "common/base/Base.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
class FetchEdgesPlanner final : public Planner {
 public:
  using EdgeProp = nebula::storage::cpp2::EdgeProp;
  using EdgeProps = std::vector<EdgeProp>;

  static std::unique_ptr<FetchEdgesPlanner> make() {
    return std::unique_ptr<FetchEdgesPlanner>(new FetchEdgesPlanner());
  }

  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kFetchEdges;
  }

  StatusOr<SubPlan> transform(AstContext* astCtx) override;

  std::unique_ptr<EdgeProps> buildEdgeProps();

  Expression* emptyEdgeFilter();

 private:
  FetchEdgesPlanner() = default;

  FetchEdgesContext* fetchCtx_{nullptr};
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_NGQL_FETCH_EDGES_H
