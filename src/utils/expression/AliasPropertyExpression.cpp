/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/AliasPropertyExpression.h"

namespace nebula {

bool AliasPropertyExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = dynamic_cast<const AliasPropertyExpression&>(rhs);
    return *ref_ == *(r.ref_) && *alias_ == *(r.alias_) && *prop_ == *(r.prop_);
}



void AliasPropertyExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // ref_
    encoder << ref_.get();

    // alias_
    encoder << alias_.get();

    // prop_
    encoder << prop_.get();
}


void AliasPropertyExpression::resetFrom(Decoder& decoder) {
    // Read ref_
    ref_ = decoder.readStr();

    // Read alias_
    alias_ = decoder.readStr();

    // Read prop_
    prop_ = decoder.readStr();
}


Value InputPropertyExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}


Value VariablePropertyExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}


Value SourcePropertyExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}


Value DestPropertyExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}


Value EdgeSrcIdExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}


Value EdgeTypeExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}


Value EdgeRankExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}


Value EdgeDstIdExpression::eval(const ExpressionContext& ctx) const {
    // TODO
    UNUSED(ctx);
    return Value(NullType::NaN);
}

}  // namespace nebula
