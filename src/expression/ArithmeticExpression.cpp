/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "expression/ArithmeticExpression.h"

namespace nebula {

Value ArithmeticExpression::eval() const {
    switch (type_) {
        case Type::EXP_ADD:
            return lhs_->eval() + rhs_->eval();
        case Type::EXP_MINUS:
            return lhs_->eval() - rhs_->eval();
        case Type::EXP_MULTIPLY:
            return lhs_->eval() * rhs_->eval();
        case Type::EXP_DIVIDE:
            return lhs_->eval() / rhs_->eval();
        case Type::EXP_MOD:
            return lhs_->eval() % rhs_->eval();
        default:
            break;
    }
    LOG(FATAL) << "Unknown type: " << type_;
}


std::string ArithmeticExpression::encode() const {
    return "";
}

}  // namespace nebula


