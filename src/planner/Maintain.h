/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MAINTAIN_H_
#define PLANNER_MAINTAIN_H_

#include "PlanNode.h"

namespace nebula {
namespace graph {
class CreateTag final : public PlanNode {
};

class CreateEdge final : public PlanNode {
};

class AlterTag final : public PlanNode {
};

class AlterEdge final : public PlanNode {
};

class DescribeTag final : public PlanNode {
};

class DescribeEdge final : public PlanNode {
};

class DropTag final : public PlanNode {
};

class DropEdge final : public PlanNode {
};

class CreateTagIndex final : public PlanNode {
};

class CreateEdgeIndex final : public PlanNode {
};

class DescribeTagIndex final : public PlanNode {
};

class DescribeEdgeIndex final : public PlanNode {
};

class DropTagIndex final : public PlanNode {
};

class DropEdgeIndex final : public PlanNode {
};

class BuildTagIndex final : public PlanNode {
};

class BuildEdgeIndex final : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MAINTAIN_H_
