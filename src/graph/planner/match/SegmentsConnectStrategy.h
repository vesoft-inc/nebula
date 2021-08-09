/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_SEGMENTSCONNECTSTRATEGY_H_
#define PLANNER_MATCH_SEGMENTSCONNECTSTRATEGY_H_

namespace nebula {
namespace graph {

class SegmentsConnectStrategy {
public:
    explicit SegmentsConnectStrategy(QueryContext* qctx) : qctx_(qctx) {}

    virtual ~SegmentsConnectStrategy() = default;

    virtual PlanNode* connect(const PlanNode* left, const PlanNode* right) = 0;

protected:
    QueryContext*   qctx_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_SEGMENTSCONNECTSTRATEGY_H_
