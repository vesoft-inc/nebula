/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_PLANNERS_MATCH_INNERJOINSTRATEGY_H_
#define GRAPH_PLANNER_PLANNERS_MATCH_INNERJOINSTRATEGY_H_

#include "graph/planner/match/SegmentsConnectStrategy.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
/*
 * The InnerJoinStrategy was designed to connect two expand part by an inner
 * join.
 */
class InnerJoinStrategy final : public SegmentsConnectStrategy {
 public:
  enum class JoinPos : int8_t { kStart, kEnd };

  explicit InnerJoinStrategy(QueryContext* qctx) : SegmentsConnectStrategy(qctx) {}

  InnerJoinStrategy* leftPos(JoinPos pos) {
    leftPos_ = pos;
    return this;
  }

  InnerJoinStrategy* rightPos(JoinPos pos) {
    rightPos_ = pos;
    return this;
  }

  PlanNode* connect(const PlanNode* left, const PlanNode* right) override;

 private:
  PlanNode* joinDataSet(const PlanNode* left, const PlanNode* right);

  JoinPos leftPos_{JoinPos::kEnd};
  JoinPos rightPos_{JoinPos::kStart};
};
}  // namespace graph
}  // namespace nebula
#endif
