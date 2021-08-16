/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_MATCH_CARTESIANPRODUCTSTRATEGY_H_
#define GRAPH_PLANNER_MATCH_CARTESIANPRODUCTSTRATEGY_H_

#include "graph/planner/match/SegmentsConnector.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
/*
 * The CartersionProductStrategy was designed to connect two unrelated subplans
 * by a CARTESIAN product.
 */

class CartesianProductStrategy final : public SegmentsConnectStrategy {
 public:
  explicit CartesianProductStrategy(QueryContext* qctx) : SegmentsConnectStrategy(qctx) {}

  PlanNode* connect(const PlanNode* left, const PlanNode* right) override;

 private:
  PlanNode* joinDataSet(const PlanNode* left, const PlanNode* right);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_CARTESIANPRODUCTSTRATEGY_H_
