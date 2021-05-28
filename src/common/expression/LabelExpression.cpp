/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/LabelExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& LabelExpression::eval(ExpressionContext&) {
    result_.setStr(name_);
    return result_;
}

std::string LabelExpression::toString() const {
    return name_;
}

bool LabelExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }
    const auto& expr = static_cast<const LabelExpression&>(rhs);
    return name_ == expr.name();
}


void LabelExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // name_
    encoder << name_;
}


void LabelExpression::resetFrom(Decoder& decoder) {
    // Read name_
    name_ = decoder.readStr();
}

void LabelExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}  // namespace nebula
