/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_ADMIN_H_
#define PLANNER_ADMIN_H_

#include "planner/PlanNode.h"

/**
 * All admin-related nodes would be put in this file.
 * These nodes would not exist in a same plan with maitain-related/
 * mutate-related/query-related nodes. And they are also isolated
 * from each other. This would be guaranteed by parser and validator.
 */
namespace nebula {
namespace graph {
class Show final : public PlanNode {
};

class CreateSpace final : public PlanNode {
};

class DropSpace final : public PlanNode {
};

class DescribeSpace final : public PlanNode {
};

class Config final : public PlanNode {
};

class Balance final : public PlanNode {
};

class CreateSnapshot final : public PlanNode {
};

class DropSnapshot final : public PlanNode {
};

class Download final : public PlanNode {
};

class Ingest final : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ADMIN_H_
