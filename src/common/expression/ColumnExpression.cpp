/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/ColumnExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& ColumnExpression::eval(ExpressionContext &ctx) {
    result_ = ctx.getColumn(index_);
    return result_;
}

bool ColumnExpression::operator==(const Expression &expr) const {
    if (kind_ != expr.kind()) {
        return false;
    }
    const auto &r = static_cast<const ColumnExpression &>(expr);
    return index_ == r.index_;
}

std::string ColumnExpression::toString() const {
    std::stringstream out;
    out << "COLUMN[" << index_ << "]";
    return out.str();
}

void ColumnExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

void ColumnExpression::writeTo(Encoder &encoder) const {
    encoder << kind_;
    encoder << index_;
}

void ColumnExpression::resetFrom(Decoder &decoder) {
    index_ = decoder.readValue().getInt();
}

}   // namespace nebula
