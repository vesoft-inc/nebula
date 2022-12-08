// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef NGQL_PLANNERS_PATHPLANNER_H
#define NGQL_PLANNERS_PATHPLANNER_H

#include "graph/context/QueryContext.h"
#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

class PathPlanner final : public Planner {
 public:
  using EdgeProp = nebula::storage::cpp2::EdgeProp;
  static std::unique_ptr<PathPlanner> make() {
    return std::unique_ptr<PathPlanner>(new PathPlanner());
  }

  static bool match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kFindPath;
  }

  StatusOr<SubPlan> transform(AstContext* astCtx) override;

 private:
  SubPlan loopDepPlan();

  PlanNode* getNeighbors(PlanNode* dep, bool reverse);

  SubPlan singlePairPlan(PlanNode* left, PlanNode* right);

  SubPlan multiPairPlan(PlanNode* left, PlanNode* right);

  StatusOr<SubPlan> allPathPlan();

  SubPlan pathInputPlan(PlanNode* dep, Starts& starts);

  PlanNode* buildPathProp(PlanNode* dep);

  // get the attributes of the vertices of the path
  PlanNode* buildVertexPlan(PlanNode* dep, const std::string& input);

  // get the attributes of the edges of the path
  PlanNode* buildEdgePlan(PlanNode* dep, const std::string& input);

 private:
  std::unique_ptr<std::vector<EdgeProp>> buildEdgeProps(bool reverse, bool withProp = false);

  void doBuildEdgeProps(std::unique_ptr<std::vector<EdgeProp>>& edgeProps,
                        bool reverse,
                        bool isInEdge,
                        bool withProp);

  void buildStart(Starts& starts, std::string& startVidsVar, bool reverse);

  Expression* singlePairLoopCondition(uint32_t steps,
                                      const std::string& pathVar,
                                      const std::string& terminateEarlyVar);

  Expression* multiPairLoopCondition(uint32_t steps, const std::string& pathVar);

  /*
   *  find path from $-.src to $-.dst
   *  startVid plan: project($-.src) <- dedup($-.src)
   *  toVid plan: project($-.dst) <- dedup($-.dst)
   *  After reconnect plans, it would be:
   *  project($-.src) <- dedup($-.src) <- project($-.dst) <- dedup($-.dst)
   *  same as find path from <vid> to $-.dst OR find path from $-.src to <vid>
   */
  SubPlan buildRuntimeVidPlan();

 private:
  PathPlanner() = default;

  PathContext* pathCtx_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  //  NGQL_PLANNERS_PATHPLANNER_H
