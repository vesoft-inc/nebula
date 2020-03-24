/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "PlanNode.h"

namespace nebula {
namespace graph {
Status PlanNode::append(std::shared_ptr<PlanNode> node,
                        std::shared_ptr<PlanNode> appended) {
    if (node->kind() == PlanNode::Kind::kEnd) {
        node.swap(appended);
    } else {
        node->addChild(appended);
    }
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
