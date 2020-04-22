/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "expression/RelationalExpression.h"

namespace nebula {
Value RelationalExpression::eval() const {
    auto lhs = lhs_->eval();
    auto rhs = rhs_->eval();

    switch (type_) {
        case Type::EXP_REL_EQ:
            return lhs_ == rhs_;
        case Type::EXP_REL_NE:
            return lhs_ != rhs_;
        case Type::EXP_REL_LT:
            return lhs_ < rhs_;
        case Type::EXP_REL_LE:
            return lhs_ <= rhs_;
        case Type::EXP_REL_GT:
            return lhs_ > rhs_;
        case Type::EXP_REL_GE:
            return lhs_ >= rhs_;
        default:
            break;
    }
    LOG(FATAL) << "Unknown type: " << type_;
}
}  // namespace nebula
