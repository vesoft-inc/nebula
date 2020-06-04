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

}  // namespace nebula
