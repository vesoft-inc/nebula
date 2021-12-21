/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_SEGMENTSCONNECTSTRATEGY_H_
#define GRAPH_PLANNER_MATCH_SEGMENTSCONNECTSTRATEGY_H_

namespace nebula {
namespace graph {

class PlanNode;
class QueryContext;

class SegmentsConnectStrategy {
 public:
  explicit SegmentsConnectStrategy(QueryContext* qctx) : qctx_(qctx) {}

  virtual ~SegmentsConnectStrategy() = default;

  virtual PlanNode* connect(const PlanNode* left, const PlanNode* right) = 0;

 protected:
  QueryContext* qctx_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_SEGMENTSCONNECTSTRATEGY_H_
