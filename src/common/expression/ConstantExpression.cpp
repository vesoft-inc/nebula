/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/ConstantExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

bool ConstantExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = static_cast<const ConstantExpression&>(rhs);
    return val_ == r.val_;
}



void ConstantExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // val_
    encoder << val_;
}


void ConstantExpression::resetFrom(Decoder& decoder) {
    // Deserialize val_
    val_ = decoder.readValue();
}

void ConstantExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

std::string ConstantExpression::toString() const {
    std::stringstream out;
    out << val_;
    return out.str();
}

}  // namespace nebula
