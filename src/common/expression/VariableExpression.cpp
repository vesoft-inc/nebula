/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/VariableExpression.h"
#include "common/datatypes/List.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {
const Value& VariableExpression::eval(ExpressionContext& ctx) {
    return ctx.getVar(var_);
}

void VariableExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

void VariableExpression::writeTo(Encoder& encoder) const {
    DCHECK(isInner_);
    encoder << kind_;
    encoder << var_;
}

void VariableExpression::resetFrom(Decoder& decoder) {
    var_ = decoder.readStr();
    isInner_ = true;
}

const Value& VersionedVariableExpression::eval(ExpressionContext& ctx) {
    if (version_ != nullptr) {
        auto version = version_->eval(ctx);
        if (UNLIKELY(!version.isInt())) {
            return Value::kNullBadType;
        }
        auto ver = version.getInt();
        return ctx.getVersionedVar(var_, ver);
    } else {
        return ctx.getVar(var_);
    }
}

std::string VariableExpression::toString() const {
    if (var_.empty()) {
        return "";
    }

    std::stringstream out;
    out << "$" << var_;
    return out.str();
}

std::string VersionedVariableExpression::toString() const {
    if (var_.empty()) {
        return "";
    }

    std::stringstream out;
    out << "$" << var_;
    if (version_ != nullptr) {
        out << "{" << version_->toString() << "}";
    }
    return out.str();
}

void VersionedVariableExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}   // namespace nebula
