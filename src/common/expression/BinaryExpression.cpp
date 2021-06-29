/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/BinaryExpression.h"

namespace nebula {

bool BinaryExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = static_cast<const BinaryExpression&>(rhs);
    return *lhs_ == *(r.lhs_) && *rhs_ == *(r.rhs_);
}


void BinaryExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // lhs_
    DCHECK(!!lhs_);
    encoder << *lhs_;

    // rhs_
    DCHECK(!!rhs_);
    encoder << *rhs_;
}

void BinaryExpression::resetFrom(Decoder& decoder) {
    // Read lhs_
    lhs_ = decoder.readExpression(pool_);
    CHECK(!!lhs_);
    // Read rhs_
    rhs_ = decoder.readExpression(pool_);
    CHECK(!!rhs_);
}

}  // namespace nebula
