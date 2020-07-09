/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan) {
    std::vector<const char*> kinds(plan.size());
    std::transform(plan.cbegin(), plan.cend(), kinds.begin(), PlanNode::toString);
    os << "[" << folly::join(", ", kinds) << "]";
    return os;
}

}   // namespace graph
}   // namespace nebula
