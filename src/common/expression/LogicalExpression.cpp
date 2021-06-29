/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/LogicalExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& LogicalExpression::eval(ExpressionContext& ctx) {
    DCHECK_GE(operands_.size(), 2UL);
    switch (kind()) {
        case Kind::kLogicalAnd:
            return evalAnd(ctx);
        case Kind::kLogicalOr:
            return evalOr(ctx);
        case Kind::kLogicalXor:
            return evalXor(ctx);
        default:
            LOG(FATAL) << "Illegal kind for logical expression: " << static_cast<int>(kind());
    }
}

// evalAnd short circuit logic: BADNULL == false > NULL >= EMPTY > true
const Value& LogicalExpression::evalAnd(ExpressionContext &ctx) {
    result_ = true;
    for (auto i = 0u; i < operands_.size(); i++) {
        auto& value = operands_[i]->eval(ctx);
        if (value.isBadNull()
            || (value.isBool() && !value.getBool())) {
            result_ = value;
            return result_;
        }
        if (!value.isBool()) {
            if (value.isNull()) {
                result_ = value;
            } else if (value.empty() && !result_.isNull()) {
                result_ = value;
            } else {
                result_ = Value::kNullBadType;
                return result_;
            }
        }
    }

    return result_;
}

// evalOr short circuit logic: BADNULL == true > NULL >= EMPTY > false
const Value& LogicalExpression::evalOr(ExpressionContext &ctx) {
    result_ = false;
    for (auto i = 0u; i < operands_.size(); i++) {
        auto& value = operands_[i]->eval(ctx);
        if (value.isBadNull()
            || (value.isBool() && value.getBool())) {
            result_ = value;
            return result_;
        }
        if (!value.isBool()) {
            if (value.isNull()) {
                result_ = value;
            } else if (value.empty() && !result_.isNull()) {
                result_ = value;
            } else {
                result_ = Value::kNullBadType;
                return result_;
            }
        }
    }

    return result_;
}

// evalXor short circuit logic: BADNULL == NULL > EMPTY > Bool
const Value& LogicalExpression::evalXor(ExpressionContext &ctx) {
    auto hasEmpty = 0u;
    auto firstBool = 1u;
    for (auto i = 0u; i < operands_.size(); i++) {
        auto &value = operands_[i]->eval(ctx);
        if (value.isNull()) {
            result_ = value;
            return result_;
        }
        if (!value.isBool()) {
            if (value.empty()) {
                result_ = value;
                hasEmpty = 1;
                continue;
            }
            result_ = Value::kNullBadType;
            return result_;
        }
        if (hasEmpty) continue;
        if (firstBool) {
            result_ = static_cast<bool>(value.getBool());
            firstBool = 0u;
        } else {
            result_ = static_cast<bool>(result_.getBool() ^ value.getBool());
        }
    }

    return result_;
}

std::string LogicalExpression::toString() const {
    std::string op;
    switch (kind()) {
        case Kind::kLogicalAnd:
            op = " AND ";
            break;
        case Kind::kLogicalOr:
            op = " OR ";
            break;
        case Kind::kLogicalXor:
            op = " XOR ";
            break;
        default:
            LOG(FATAL) << "Illegal kind for logical expression: " << static_cast<int>(kind());
    }
    std::string buf;
    buf.reserve(256);

    buf += "(";
    buf += operands_[0]->toString();
    for (auto i = 1u; i < operands_.size(); i++) {
        buf += op;
        buf += operands_[i]->toString();
    }
    buf += ")";

    return buf;
}

void LogicalExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

void LogicalExpression::writeTo(Encoder &encoder) const {
    encoder << kind();
    encoder << operands_.size();
    for (auto &expr : operands_) {
        encoder << *expr;
    }
}

void LogicalExpression::resetFrom(Decoder &decoder) {
    auto size = decoder.readSize();
    operands_.resize(size);
    for (auto i = 0u; i < size; i++) {
        operands_[i] = decoder.readExpression(pool_);
    }
}

bool LogicalExpression::operator==(const Expression &rhs) const {
    if (kind() != rhs.kind()) {
        return false;
    }
    auto &logic = static_cast<const LogicalExpression&>(rhs);

    if (operands_.size() != logic.operands_.size()) {
        return false;
    }

    for (auto i = 0u; i < operands_.size(); i++) {
        if (*operands_[i] != *logic.operands_[i]) {
            return false;
        }
    }

    return true;
}

}  // namespace nebula
