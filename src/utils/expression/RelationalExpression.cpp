/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/RelationalExpression.h"

namespace nebula {

Value RelationalExpression::eval(const ExpressionContext& ctx) const {
    UNUSED(ctx);

    auto lhs = lhs_->eval(ctx);
    auto rhs = rhs_->eval(ctx);

    switch (kind_) {
        case Kind::kRelEQ:
            return lhs_ == rhs_;
        case Kind::kRelNE:
            return lhs_ != rhs_;
        case Kind::kRelLT:
            return lhs_ < rhs_;
        case Kind::kRelLE:
            return lhs_ <= rhs_;
        case Kind::kRelGT:
            return lhs_ > rhs_;
        case Kind::kRelGE:
            return lhs_ >= rhs_;
        default:
            break;
    }
    LOG(FATAL) << "Unknown type: " << kind_;
}

}  // namespace nebula
