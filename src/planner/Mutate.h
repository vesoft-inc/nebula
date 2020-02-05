/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MUTATE_H_
#define PLANNER_MUTATE_H_

#include "PlanNode.h"

/**
 * All mutate-related nodes would put in this file.
 */
namespace nebula {
namespace graph {
class InsertVertex final : public PlanNode {
};

class InsertEdge final : public PlanNode {
};

class UpdateVertex final : public PlanNode {
};

class UpdateEdge final : public PlanNode {
};

class DeleteVertex final : public PlanNode {
};

class DeleteEdge final : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MUTATE_H_
