/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_NGQL_GOPLANNER_H_
#define GRAPH_PLANNER_NGQL_GOPLANNER_H_

#include "common/base/Base.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

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
  SubPlan doPlan();

  SubPlan doSimplePlan();

 private:
  std::unique_ptr<VertexProps> buildVertexProps(const ExpressionProps::TagIDPropsMap& propsMap);

  std::unique_ptr<EdgeProps> buildEdgeProps(bool onlyDst);

  void doBuildEdgeProps(std::unique_ptr<EdgeProps>& edgeProps, bool onlyDst, bool isInEdge);

  PlanNode* buildJoinDstPlan(PlanNode* dep);

  std::vector<EdgeType> buildEdgeTypes();

 private:
  GoPlanner() = default;

  GoContext* goCtx_{nullptr};
  // runtime : argument, else : startNode
  PlanNode* startNode_{nullptr};
  PlanNode* preRootNode_{nullptr};

  const int16_t VID_INDEX = 0;
  const int16_t LAST_COL_INDEX = -1;
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_NGQL_GOPLANNER_H
