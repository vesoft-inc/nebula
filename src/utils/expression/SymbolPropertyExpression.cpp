/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/SymbolPropertyExpression.h"

namespace nebula {


bool SymbolPropertyExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = dynamic_cast<const SymbolPropertyExpression&>(rhs);
    return *ref_ == *(r.ref_) && *sym_ == *(r.sym_) && *prop_ == *(r.prop_);
}


void SymbolPropertyExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // ref_
    encoder << ref_.get();

    // alias_
    encoder << sym_.get();

    // prop_
    encoder << prop_.get();
}


void SymbolPropertyExpression::resetFrom(Decoder& decoder) {
    // Read ref_
    ref_ = decoder.readStr();

    // Read alias_
    sym_ = decoder.readStr();

    // Read prop_
    prop_ = decoder.readStr();
}


const Value& EdgePropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getEdgeProp(*sym_, *prop_);
}


const Value& InputPropertyExpression::eval(ExpressionContext& ctx) {
    // TODO
    return ctx.getInputProp(*prop_);
}


const Value& VariablePropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getVarProp(*sym_, *prop_);
}


const Value& SourcePropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getSrcProp(*sym_, *prop_);
}


const Value& DestPropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getDstProp(*sym_, *prop_);
}


const Value& EdgeSrcIdExpression::eval(ExpressionContext& ctx) {
    return ctx.getEdgeProp(*sym_, *prop_);
}


const Value& EdgeTypeExpression::eval(ExpressionContext& ctx) {
    return ctx.getEdgeProp(*sym_, *prop_);
}


const Value& EdgeRankExpression::eval(ExpressionContext& ctx) {
    return ctx.getEdgeProp(*sym_, *prop_);
}


const Value& EdgeDstIdExpression::eval(ExpressionContext& ctx) {
    return ctx.getEdgeProp(*sym_, *prop_);
}
}  // namespace nebula
