/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/PropertyExpression.h"

namespace nebula {


bool PropertyExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = dynamic_cast<const PropertyExpression&>(rhs);
    return *ref_ == *(r.ref_) && *sym_ == *(r.sym_) && *prop_ == *(r.prop_);
}


void PropertyExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // ref_
    encoder << ref_.get();

    // alias_
    encoder << sym_.get();

    // prop_
    encoder << prop_.get();
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
    result_ = ctx.getEdgeProp(*sym_, *prop_);
    return result_;
}

const Value& TagPropertyExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getTagProp(*sym_, *prop_);
    return result_;
}

const Value& InputPropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getInputProp(*prop_);
}


const Value& VariablePropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getVarProp(*sym_, *prop_);
}


const Value& SourcePropertyExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getSrcProp(*sym_, *prop_);
    return result_;
}


const Value& DestPropertyExpression::eval(ExpressionContext& ctx) {
    return ctx.getDstProp(*sym_, *prop_);
}


const Value& EdgeSrcIdExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(*sym_, *prop_);
    return result_;
}


const Value& EdgeTypeExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(*sym_, *prop_);
    return result_;
}


const Value& EdgeRankExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(*sym_, *prop_);
    return result_;
}


const Value& EdgeDstIdExpression::eval(ExpressionContext& ctx) {
    result_ = ctx.getEdgeProp(*sym_, *prop_);
    return result_;
}

std::string PropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (ref_ != nullptr && !ref_->empty()) {
        buf += *ref_;
        buf += ".";
    }

    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string EdgePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string TagPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string InputPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (ref_ != nullptr && !ref_->empty()) {
        buf += *ref_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string VariablePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (ref_ != nullptr) {
        buf += *ref_;
    }
    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string SourcePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (ref_ != nullptr && !ref_->empty()) {
        buf += *ref_;
        buf += ".";
    }
    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string DestPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (ref_ != nullptr && !ref_->empty()) {
        buf += *ref_;
        buf += ".";
    }
    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string EdgeSrcIdExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string EdgeTypeExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string EdgeRankExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

std::string EdgeDstIdExpression::toString() const {
    std::string buf;
    buf.reserve(64);

    if (sym_ != nullptr && !sym_->empty()) {
        buf += *sym_;
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }

    return buf;
}

}  // namespace nebula
