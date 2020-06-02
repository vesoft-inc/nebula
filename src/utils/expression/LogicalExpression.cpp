/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/LogicalExpression.h"

namespace nebula {

Value LogicalExpression::eval(const ExpressionContext& ctx) const {
    UNUSED(ctx);

    auto lhs = lhs_->eval(ctx);
    auto rhs = rhs_->eval(ctx);

    switch (kind_) {
        case Kind::kLogicalAnd:
            return lhs && rhs;
        case Kind::kLogicalOr:
            return lhs || rhs;
        case Kind::kLogicalXor:
            return (lhs && !rhs) || (!lhs && rhs);
        default:
            break;
    }
    LOG(FATAL) << "Unknown type: " << kind_;
}

}  // namespace nebula
