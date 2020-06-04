/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/TypeCastingExpression.h"

namespace nebula {
const Value& TypeCastingExpression::eval(ExpressionContext& ctx) {
    UNUSED(ctx);
    return result_;
}


bool TypeCastingExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = dynamic_cast<const TypeCastingExpression&>(rhs);
    return vType_ == r.vType_ && *operand_ == *(r.operand_);
}


void TypeCastingExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // vType_
    encoder << vType_;

    // operand_
    DCHECK(!!operand_);
    encoder << *operand_;
}


void TypeCastingExpression::resetFrom(Decoder& decoder) {
    // Read vType_
    vType_ = decoder.readValueType();

    // Read operand_
    operand_ = decoder.readExpression();
    CHECK(!!operand_);
}
}  // namespace nebula
