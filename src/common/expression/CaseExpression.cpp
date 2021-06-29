/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/CaseExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Set.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

bool CaseExpression::operator==(const Expression& rhs) const {
    if (kind() != rhs.kind()) {
        return false;
    }

    const auto& expr = static_cast<const CaseExpression&>(rhs);
    if (isGeneric() != expr.isGeneric()) {
        return false;
    }
    if (hasCondition() != expr.hasCondition()) {
        return false;
    }
    if (hasCondition()) {
        if (*condition_ != *expr.condition_) {
            return false;
        }
    }
    if (hasDefault() != expr.hasDefault()) {
        return false;
    }
    if (hasDefault()) {
        if (*default_ != *expr.default_) {
            return false;
        }
    }
    if (numCases() != expr.numCases()) {
        return false;
    }
    for (auto i = 0u; i < numCases(); i++) {
        if (*cases_[i].when != *expr.cases_[i].when) {
            return false;
        }
        if (*cases_[i].then != *expr.cases_[i].then) {
            return false;
        }
    }

    return true;
}

const Value& CaseExpression::eval(ExpressionContext& ctx) {
    auto cond = condition_ != nullptr ? condition_->eval(ctx) : Value();
    for (const auto& whenThen : cases_) {
        auto when = whenThen.when->eval(ctx);
        if (condition_ != nullptr) {
            if (cond == when) {
                result_ = whenThen.then->eval(ctx);
                return result_;
            }
        } else {
            if (!when.isBool()) {
                return Value::kNullBadType;
            }
            if (when.getBool()) {
                result_ = whenThen.then->eval(ctx);
                return result_;
            }
        }
    }
    if (default_ != nullptr) {
        result_ = default_->eval(ctx);
    } else {
        result_ = Value::kNullValue;
    }

    return result_;
}

std::string CaseExpression::toString() const {
    std::string buf;
    buf.reserve(256);

    if (isGeneric_) {
        buf += "CASE";
        if (condition_ != nullptr) {
            buf += " ";
            buf += condition_->toString();
        }
        for (const auto& whenThen : cases_) {
            buf += " WHEN ";
            buf += whenThen.when->toString();
            buf += " THEN ";
            buf += whenThen.then->toString();
        }
        if (default_ != nullptr) {
            buf += " ELSE ";
            buf += default_->toString();
        }
        buf += " END";
    } else {
        buf += "(";
        buf += cases_.front().when->toString();
        buf += " ? ";
        buf += cases_.front().then->toString();
        buf += " : ";
        buf += default_->toString();
        buf += ")";
    }

    return buf;
}

void CaseExpression::writeTo(Encoder& encoder) const {
    encoder << kind_;
    encoder << Value(isGeneric_);
    encoder << Value(condition_ != nullptr);
    encoder << Value(default_ != nullptr);
    encoder << cases_.size();
    if (condition_ != nullptr) {
        encoder << *condition_;
    }
    if (default_ != nullptr) {
        encoder << *default_;
    }
    for (const auto& whenThen : cases_) {
        encoder << *whenThen.when;
        encoder << *whenThen.then;
    }
}

void CaseExpression::resetFrom(Decoder& decoder) {
    bool isGeneric = decoder.readValue().getBool();
    bool hasCondition = decoder.readValue().getBool();
    bool hasDefault = decoder.readValue().getBool();
    auto numCases = decoder.readSize();

    isGeneric_ = isGeneric;
    if (hasCondition) {
        condition_ = decoder.readExpression(pool_);
        CHECK(!!condition_);
    }
    if (hasDefault) {
        default_ = decoder.readExpression(pool_);
        CHECK(!!default_);
    }
    cases_.reserve(numCases);
    for (auto i = 0u; i < numCases; i++) {
        auto when = decoder.readExpression(pool_);
        CHECK(!!when);
        auto then = decoder.readExpression(pool_);
        CHECK(!!then);
        cases_.emplace_back(when, then);
    }
}

void CaseExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}   // namespace nebula
