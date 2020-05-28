/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/UnaryExpression.h"

namespace nebula {
Value UnaryExpression::eval() const {
    switch (kind_) {
        case Kind::kUnaryPlus:
            return operand_->eval();
        case Kind::kUnaryNegate:
            return -(operand_->eval());
        case Kind::kUnaryNot:
            return !(operand_->eval());
        default:
            break;
    }
    LOG(FATAL) << "Unknown type: " << kind_;
}
}   // namespace nebula
