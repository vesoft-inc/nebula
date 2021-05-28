/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/PropertyExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {


bool PropertyExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = static_cast<const PropertyExpression&>(rhs);
    return ref_ == r.ref_ && sym_ == r.sym_ && prop_ == r.prop_;
}


void PropertyExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // ref_
    encoder << ref_;

    // alias_
    encoder << sym_;

    // prop_
    encoder << prop_;
}


void PropertyExpression::resetFrom(Decoder& decoder) {
    // Read ref_
    ref_ = decoder.readStr();

    // Read alias_
    sym_ = decoder.readStr();

    // Read prop_
    prop_ = decoder.readStr();
}

const Value& PropertyExpression::eval(ExpressionContext& ctx) {
    // TODO maybe cypher need it.
    UNUSED(ctx);
    LOG(FATAL) << "Unimplemented";
}

const Value& EdgePropertyExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(sym_, prop_);
    return result_;
}

void EdgePropertyExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

const Value& TagPropertyExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getTagProp(sym_, prop_);
    return result_;
}

void TagPropertyExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

const Value& InputPropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getInputProp(prop_);
}

void InputPropertyExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

const Value& VariablePropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getVarProp(sym_, prop_);
}

void VariablePropertyExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

const Value& SourcePropertyExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getSrcProp(sym_, prop_);
    return result_;
}

void SourcePropertyExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

const Value& DestPropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getDstProp(sym_, prop_);
}

void DestPropertyExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

const Value& EdgeSrcIdExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(sym_, prop_);
    return result_;
}

void EdgeSrcIdExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

const Value& EdgeTypeExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(sym_, prop_);
    return result_;
}

void EdgeTypeExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

const Value& EdgeRankExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(sym_, prop_);
    return result_;
}

void EdgeRankExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

const Value& EdgeDstIdExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(sym_, prop_);
    return result_;
}

void EdgeDstIdExpression::accept(ExprVisitor * visitor) {
    visitor->visit(this);
}

std::string PropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (!ref_.empty()) {
        buf += ref_;
        buf += ".";
    }

    if (!sym_.empty()) {
        buf += sym_;
        buf += ".";
    }
    if (!prop_.empty()) {
        buf += prop_;
    }

    return buf;
}

std::string VariablePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (!ref_.empty()) {
        buf += ref_;
    }
    if (!sym_.empty()) {
        buf += sym_;
        buf += ".";
    }
    if (!prop_.empty()) {
        buf += prop_;
    }

    return buf;
}

}  // namespace nebula
