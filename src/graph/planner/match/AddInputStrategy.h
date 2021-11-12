/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_ADDINPUTSTRATEGY_H_
#define GRAPH_PLANNER_MATCH_ADDINPUTSTRATEGY_H_

#include "graph/planner/match/SegmentsConnectStrategy.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
/*
 * The AddInputStrategy was designed to connect two subplan by adding
 * dependency.
 */
class AddInputStrategy final : public SegmentsConnectStrategy {
 public:
  explicit AddInputStrategy(bool copyColNames)
      : SegmentsConnectStrategy(nullptr), copyColNames_(copyColNames) {}

  PlanNode* connect(const PlanNode* left, const PlanNode* right) override;

 private:
  bool copyColNames_{false};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_SIMPLECONNECTSTRATEGY_H_
