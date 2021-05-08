/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_ADDDEPENDENCYSTRATEGY_H_
#define PLANNER_MATCH_ADDDEPENDENCYSTRATEGY_H_

#include "planner/plan/PlanNode.h"
#include "planner/match/SegmentsConnectStrategy.h"

namespace nebula {
namespace graph {
/*
 * The AddDependencyStrategy was designed to connect two subplan by adding dependency.
 */
class AddDependencyStrategy final : public SegmentsConnectStrategy {
public:
    AddDependencyStrategy() : SegmentsConnectStrategy(nullptr) {}

    PlanNode* connect(const PlanNode* left, const PlanNode* right) override;
};
}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_MATCH_SIMPLECONNECTSTRATEGY_H_
