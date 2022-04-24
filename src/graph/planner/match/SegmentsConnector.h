/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_SEGMENTSCONNECTOR_H_
#define GRAPH_PLANNER_MATCH_SEGMENTSCONNECTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
// The SegmentsConnector is a util to help connecting the plan segment.
class SegmentsConnector final {
 public:
  SegmentsConnector() = delete;

  /**
   * InnerJoin two plan on node id
   */
  static SubPlan innerJoin(QueryContext* qctx,
                           const SubPlan& left,
                           const SubPlan& right,
                           const std::unordered_set<std::string>& intersectedAliases);

  /**
   * LeftJoin two plan on node id
   */
  static SubPlan leftJoin(QueryContext* qctx,
                          const SubPlan& left,
                          const SubPlan& right,
                          const std::unordered_set<std::string>& intersectedAliases);

  /**
   * Simply do a CartesianProduct of two plan root.
   */
  static SubPlan cartesianProduct(QueryContext* qctx, const SubPlan& left, const SubPlan& right);

  static SubPlan rollUpApply(QueryContext* qctx,
                             const SubPlan& left,
                             const std::vector<std::string>& inputColNames,
                             const SubPlan& right,
                             const std::vector<std::string>& compareCols,
                             const std::string& collectCol);

  /*
   * left->right
   */
  static SubPlan addInput(const SubPlan& left, const SubPlan& right, bool copyColNames = false);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_SEGMENTSCONNECTOR_H_
