/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "PlanNode.h"

namespace nebula {
namespace graph {
Status PlanNode::append(std::shared_ptr<PlanNode> node) {
    UNUSED(node);
    return Status::OK();
}

Status PlanNode::merge(std::shared_ptr<StartNode> start) {
    UNUSED(start);
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
