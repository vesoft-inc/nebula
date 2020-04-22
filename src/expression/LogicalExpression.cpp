/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "expression/LogicalExpression.h"

namespace nebula {
Value LogicalExpression::eval() const {
    auto lhs = lhs_->eval();
    auto rhs = rhs_->eval();

    switch (type_) {
        case Type::EXP_LOGICAL_AND:
            return lhs && rhs;
        case Type::EXP_LOGICAL_OR:
            return lhs || rhs;
        case Type::EXP_LOGICAL_XOR:
            return (lhs && !rhs) || (!lhs && rhs);
        default:
            break;
    }
    LOG(FATAL) << "Unknown type: " << type_;
}
}  // namespace nebula
