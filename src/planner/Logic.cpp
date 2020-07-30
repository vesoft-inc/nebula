/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Logic.h"

namespace nebula {
namespace graph {

std::string Select::explain() const {
    return "Select";
}

Loop::Loop(ExecutionPlan* plan, PlanNode* input, PlanNode* body, Expression* condition)
    : BinarySelect(plan, Kind::kLoop, input, condition), body_(body) {}

std::string Loop::explain() const {
    return "Loop";
}

}   // namespace graph
}   // namespace nebula
