/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/ReduceExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& ReduceExpression::eval(ExpressionContext& ctx) {
    auto& initVal = initial_->eval(ctx);

    auto& listVal = collection_->eval(ctx);
    if (listVal.isNull() || listVal.empty()) {
        result_ = listVal;
        return result_;
    }
    if (!listVal.isList()) {
        result_ = Value::kNullBadType;
        return result_;
    }
    auto& list = listVal.getList();

    ctx.setVar(accumulator_, initVal);
    for (size_t i = 0; i < list.size(); ++i) {
        auto& v = list[i];
        ctx.setVar(innerVar_, v);
        auto& mappingVal = mapping_->eval(ctx);
        ctx.setVar(accumulator_, mappingVal);
    }

    result_ = ctx.getVar(accumulator_);
    return result_;
}

Expression* ReduceExpression::clone() const {
    auto expr = ReduceExpression::make(pool_,
                                       accumulator_,
                                       initial_->clone(),
                                       innerVar_,
                                       collection_->clone(),
                                       mapping_ != nullptr ? mapping_->clone() : nullptr);
    if (hasOriginString()) {
        expr->setOriginString(originString_);
    }
    return expr;
}

bool ReduceExpression::operator==(const Expression& rhs) const {
    if (kind() != rhs.kind()) {
        return false;
    }

    const auto& expr = static_cast<const ReduceExpression&>(rhs);

    if (accumulator_ != expr.accumulator_) {
        return false;
    }

    if (*initial_ != *expr.initial_) {
        return false;
    }

    if (innerVar_ != expr.innerVar_) {
        return false;
    }

    if (*collection_ != *expr.collection_) {
        return false;
    }
    if (*mapping_ != *expr.mapping_) {
        return false;
    }

    return true;
}

void ReduceExpression::writeTo(Encoder& encoder) const {
    encoder << kind_;
    encoder << Value(hasOriginString());

    encoder << accumulator_;
    encoder << *initial_;
    encoder << innerVar_;
    encoder << *collection_;
    encoder << *mapping_;
    if (hasOriginString()) {
        encoder << originString_;
    }
}

void ReduceExpression::resetFrom(Decoder& decoder) {
    bool hasString = decoder.readValue().getBool();

    accumulator_ = decoder.readStr();
    initial_ = decoder.readExpression(pool_);
    innerVar_ = decoder.readStr();
    collection_ = decoder.readExpression(pool_);
    mapping_ = decoder.readExpression(pool_);
    if (hasString) {
        originString_ = decoder.readStr();
    }
}

std::string ReduceExpression::toString() const {
    std::string buf;
    buf.reserve(256);

    buf += "reduce";
    buf += "(";
    buf += accumulator_;
    buf += " = ";
    buf += initial_->toString();
    buf += ", ";
    buf += innerVar_;
    buf += " IN ";
    buf += collection_->toString();
    buf += " | ";
    buf += mapping_->toString();
    buf += ")";

    return buf;
}

void ReduceExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}   // namespace nebula
