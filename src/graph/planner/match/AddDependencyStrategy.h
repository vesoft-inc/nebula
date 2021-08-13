/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_MATCH_ADDDEPENDENCYSTRATEGY_H_
#define GRAPH_PLANNER_MATCH_ADDDEPENDENCYSTRATEGY_H_

#include "graph/planner/match/SegmentsConnectStrategy.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
/*
 * The AddDependencyStrategy was designed to connect two subplan by adding
 * dependency.
 */
class AddDependencyStrategy final : public SegmentsConnectStrategy {
 public:
  AddDependencyStrategy() : SegmentsConnectStrategy(nullptr) {}

  PlanNode* connect(const PlanNode* left, const PlanNode* right) override;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_SIMPLECONNECTSTRATEGY_H_
