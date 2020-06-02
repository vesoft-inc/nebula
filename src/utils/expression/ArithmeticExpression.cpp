/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/ArithmeticExpression.h"

namespace nebula {

Value ArithmeticExpression::eval(const ExpressionContext& ctx) const {
    UNUSED(ctx);
    switch (kind_) {
        case Kind::kAdd:
            return lhs_->eval(ctx) + rhs_->eval(ctx);
        case Kind::kMinus:
            return lhs_->eval(ctx) - rhs_->eval(ctx);
        case Kind::kMultiply:
            return lhs_->eval(ctx) * rhs_->eval(ctx);
        case Kind::kDivision:
            return lhs_->eval(ctx) / rhs_->eval(ctx);
        case Kind::kMod:
            return lhs_->eval(ctx) % rhs_->eval(ctx);
        default:
            break;
    }
    LOG(FATAL) << "Unknown type: " << kind_;
}

}  // namespace nebula
