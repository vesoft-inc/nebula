/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/LogicalExpression.h"

namespace nebula {
const Value& LogicalExpression::eval(ExpressionContext& ctx) {
    auto& lhs = lhs_->eval(ctx);
    auto& rhs = rhs_->eval(ctx);

    switch (kind_) {
        case Kind::kLogicalAnd:
            result_ = lhs && rhs;
            break;
        case Kind::kLogicalOr:
            result_ = lhs || rhs;
            break;
        case Kind::kLogicalXor:
            result_ = (lhs && !rhs) || (!lhs && rhs);
            break;
        default:
            LOG(FATAL) << "Unknown type: " << kind_;
    }
    return result_;
}

std::string LogicalExpression::toString() const {
    std::string op;
    switch (kind_) {
        case Kind::kLogicalAnd:
            op = "&&";
            break;
        case Kind::kLogicalOr:
            op = "||";
            break;
        case Kind::kLogicalXor:
            op = "XOR";
            break;
        default:
            op = "illegal symbol ";
    }
    std::stringstream out;
    out << "(" << lhs_->toString() << op << rhs_->toString() << ")";
    return out.str();
}

}  // namespace nebula
