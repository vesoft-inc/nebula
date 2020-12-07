/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLANNERS_MATCH_INNERJOINSTRATEGY_H_
#define PLANNER_PLANNERS_MATCH_INNERJOINSTRATEGY_H_

#include "planner/match/SegmentsConnector.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {
/*
 * The InnerJoinStrategy was designed to connect two expand part by an inner join.
 */
class InnerJoinStrategy final : public SegmentsConnectStrategy {
public:
    explicit InnerJoinStrategy(QueryContext* qctx) : SegmentsConnectStrategy(qctx) {}

    PlanNode* connect(const PlanNode* left, const PlanNode* right) override;

private:
    PlanNode* joinDataSet(const PlanNode* left, const PlanNode* right);
};
}  // namespace graph
}  // namespace nebula
#endif
