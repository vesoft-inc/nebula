/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/UUIDExpression.h"

namespace nebula {

bool UUIDExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = dynamic_cast<const UUIDExpression&>(rhs);
    return *field_ == *(r.field_);
}



void UUIDExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // field_
    CHECK(!!field_);
    encoder << field_.get();
}


void UUIDExpression::resetFrom(Decoder& decoder) {
    // Read field_
    field_ = decoder.readStr();
}


Value UUIDExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}

}  // namespace nebula
