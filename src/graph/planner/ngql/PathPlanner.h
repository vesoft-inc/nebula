/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NGQL_PLANNERS_PATHPLANNER_H
#define NGQL_PLANNERS_PATHPLANNER_H

#include "context/QueryContext.h"
#include "context/ast/QueryAstContext.h"
#include "planner/Planner.h"
#include "planner/plan/Query.h"

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
    SubPlan singlePairPlan(PlanNode* dep);

    SubPlan multiPairPlan(PlanNode* dep);

    SubPlan allPairPlan(PlanNode* dep);

    PlanNode* singlePairPath(PlanNode* dep, bool reverse);

    PlanNode* multiPairPath(PlanNode* dep, bool reverse);

    PlanNode* allPairPath(PlanNode* dep, bool reverse);

    PlanNode* buildPathProp(PlanNode* dep);

    // get the attributes of the vertices of the path
    PlanNode* buildVertexPlan(PlanNode* dep, const std::string& input);

    // get the attributes of the edges of the path
    PlanNode* buildEdgePlan(PlanNode* dep, const std::string& input);

private:
    std::unique_ptr<std::vector<EdgeProp>> buildEdgeProps(bool reverse);

    void doBuildEdgeProps(std::unique_ptr<std::vector<EdgeProp>>& edgeProps,
                          bool reverse,
                          bool isInEdge);

    void buildStart(Starts& starts, std::string& startVidsVar, bool reverse);

    Expression* singlePairLoopCondition(uint32_t steps, const std::string& pathVar);

    Expression* multiPairLoopCondition(uint32_t steps, const std::string& pathVar);

    Expression* allPairLoopCondition(uint32_t steps);

    /*
     *  find path from $-.src to $-.dst
     *  startVid plan: project($-.src) <- dedup($-.src)
     *  toVid plan: project($-.dst) <- dedup($-.dst)
     *  After reconnect plans, it would be:
     *  project($-.src) <- dedup($-.src) <- project($-.dst) <- dedup($-.dst)
     *  same as find path from <vid> to $-.dst OR find path from $-.src to <vid>
     */
    SubPlan buildRuntimeVidPlan();

    /*
     * When the number of steps is odd
     * For example: A->B start: A,  end: B
     * we start to expand from the starting point and the ending point at the same time
     * expand from start is pathA : A->B
     * expand from to is pathB : B->A
     * When conjunct paths
     * we should determine whether the end B and end point of pathA are equal first
     * then determine whether the end point of pathB and the end point of pathA are equal
     * so we should build path(B) and path(A)
     */
    PlanNode* allPairStartVidDataSet(PlanNode* dep, const std::string& input);

    // refer to allPairStartVidDataSet
    PlanNode* multiPairStartVidDataSet(PlanNode* dep, const std::string& input);

    SubPlan multiPairLoopDepPlan();

    SubPlan allPairLoopDepPlan();

private:
    PathPlanner() = default;

    PathContext* pathCtx_{nullptr};
};

}  // namespace graph
}  // namespace nebula
#endif  //  NGQL_PLANNERS_PATHPLANNER_H
