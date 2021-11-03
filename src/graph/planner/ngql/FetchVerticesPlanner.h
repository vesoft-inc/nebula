/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_NGQL_FETCH_VERTICES_PLANNER_H_
#define GRAPH_PLANNER_NGQL_FETCH_VERTICES_PLANNER_H_

#include "common/base/Base.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
class FetchVerticesPlanner final : public Planner {
 public:
  using VertexProp = nebula::storage::cpp2::VertexProp;
  using VertexProps = std::vector<VertexProp>;

  static std::unique_ptr<FetchVerticesPlanner> make() {
    return std::unique_ptr<FetchVerticesPlanner>(new FetchVerticesPlanner());
  }

  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kFetchVertices;
  }

  StatusOr<SubPlan> transform(AstContext* astCtx) override;

 private:
  std::unique_ptr<VertexProps> buildVertexProps(const ExpressionProps::TagIDPropsMap& propsMap);

 private:
  FetchVerticesPlanner() = default;

  FetchVerticesContext* fetchCtx_{nullptr};
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_NGQL_FETCH_VERTICES_PLANNER_H
