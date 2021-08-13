/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_MATCH_SEGMENTSCONNECTOR_H_
#define GRAPH_PLANNER_MATCH_SEGMENTSCONNECTOR_H_

#include "graph/context/QueryContext.h"
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"
#include "graph/planner/match/InnerJoinStrategy.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
/**
 * The SegmentsConnector was designed to be a util to help connecting the
 * plan segment.
 */
class SegmentsConnector final {
 public:
  enum class ConnectStrategy : int8_t {
    kAddDependency,
    kInnerJoin,
    kLeftOuterJoin,
    kCartesianProduct,
    kUnion,
  };

  SegmentsConnector() = delete;

  // Analyse the relation of two segments and connect them.
  static StatusOr<SubPlan> connectSegments(CypherClauseContextBase* leftCtx,
                                           CypherClauseContextBase* rightCtx,
                                           SubPlan& left,
                                           SubPlan& right);

  static PlanNode* innerJoinSegments(
      QueryContext* qctx,
      const PlanNode* left,
      const PlanNode* right,
      InnerJoinStrategy::JoinPos leftPos = InnerJoinStrategy::JoinPos::kEnd,
      InnerJoinStrategy::JoinPos rightPos = InnerJoinStrategy::JoinPos::kStart);

  static PlanNode* cartesianProductSegments(QueryContext* qctx,
                                            const PlanNode* left,
                                            const PlanNode* right);

  static void addDependency(const PlanNode* left, const PlanNode* right);

  static void addInput(const PlanNode* left, const PlanNode* right, bool copyColNames = false);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_SEGMENTSCONNECTOR_H_
