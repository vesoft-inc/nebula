/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_ADDINPUTSTRATEGY_H_
#define PLANNER_MATCH_ADDINPUTSTRATEGY_H_

#include "planner/plan/PlanNode.h"
#include "planner/match/SegmentsConnectStrategy.h"

namespace nebula {
namespace graph {
/*
 * The AddInputStrategy was designed to connect two subplan by adding dependency.
 */
class AddInputStrategy final : public SegmentsConnectStrategy {
public:
    explicit AddInputStrategy(bool copyColNames)
        : SegmentsConnectStrategy(nullptr), copyColNames_(copyColNames) {}

    PlanNode* connect(const PlanNode* left, const PlanNode* right) override;

private:
    bool copyColNames_{false};
};
}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_MATCH_SIMPLECONNECTSTRATEGY_H_
