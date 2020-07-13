/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/RelationalExpression.h"
#include "common/datatypes/List.h"

namespace nebula {
const Value& RelationalExpression::eval(ExpressionContext& ctx) {
    auto& lhs = lhs_->eval(ctx);
    auto& rhs = rhs_->eval(ctx);

    if (kind_ != Kind::kRelEQ && kind_ != Kind::kRelNE) {
        auto lhsEmptyOrNull = lhs.type() & Value::kEmptyNullType;
        auto rhsEmptyOrNull = rhs.type() & Value::kEmptyNullType;
        if (lhsEmptyOrNull || rhsEmptyOrNull) {
            return lhsEmptyOrNull ? lhs : rhs;
        }
    }
    switch (kind_) {
        case Kind::kRelEQ:
            result_ = lhs == rhs;
            break;
        case Kind::kRelNE:
            result_ = lhs != rhs;
            break;
        case Kind::kRelLT:
            result_ = lhs < rhs;
            break;
        case Kind::kRelLE:
            result_ = lhs <= rhs;
            break;
        case Kind::kRelGT:
            result_ = lhs > rhs;
            break;
        case Kind::kRelGE:
            result_ = lhs >= rhs;
            break;
        case Kind::kRelIn: {
            if (UNLIKELY(rhs.type() != Value::Type::LIST)) {
                result_ = Value(NullType::BAD_TYPE);
                break;
            }
            auto& list = rhs.getList().values;
            auto found = std::find(list.begin(), list.end(), lhs);
            if (found == list.end()) {
                result_ = false;
            } else {
                result_ = true;
            }
            break;
        }
        default:
            LOG(FATAL) << "Unknown type: " << kind_;
    }
    return result_;
}

std::string RelationalExpression::toString() const {
    std::string op;
    switch (kind_) {
        case Kind::kRelLT:
            op = "<";
            break;
        case Kind::kRelLE:
            op = "<=";
            break;
        case Kind::kRelGT:
            op = ">";
            break;
        case Kind::kRelGE:
            op = ">=";
            break;
        case Kind::kRelEQ:
            op = "==";
            break;
        case Kind::kRelNE:
            op = "!=";
            break;
        case Kind::kRelIn:
            op = " IN ";
            break;
        default:
            op = "illegal symbol ";
    }
    std::stringstream out;
    out << "(" << lhs_->toString() << op << rhs_->toString() << ")";
    return out.str();
}

}  // namespace nebula
