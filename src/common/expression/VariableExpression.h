/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_VARIABLEEXPRESSION_H_
#define COMMON_EXPRESSION_VARIABLEEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {
class VariableExpression final : public Expression {
public:
    explicit VariableExpression(const std::string& var = "", bool isInner = false)
        : Expression(Kind::kVar), isInner_(isInner), var_(var) {}

    const std::string& var() const {
        return var_;
    }

    bool isInner() const {
        return isInner_;
    }

    const Value& eval(ExpressionContext& ctx) override;

    bool operator==(const Expression& rhs) const override {
        if (kind() != rhs.kind()) {
            return false;
        }
        return var() == static_cast<const VariableExpression&>(rhs).var();
    }

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<VariableExpression>(var(), isInner_);
    }

private:
    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

    bool isInner_{false};
    std::string var_;
};

/*
 * VersionedVariableExpression is designed for getting the historical results
 * of a variable.
 */
class VersionedVariableExpression final : public Expression {
public:
    explicit VersionedVariableExpression(const std::string& var = "", Expression* version = nullptr)
        : Expression(Kind::kVersionedVar), var_(var) {
        version_.reset(version);
    }

    const std::string& var() const {
        return var_;
    }

    const Value& eval(ExpressionContext& ctx) override;

    bool operator==(const Expression& rhs) const override {
        if (kind() != rhs.kind()) {
            return false;
        }

        auto &vve = static_cast<const VersionedVariableExpression&>(rhs);
        if (var() != vve.var()) {
            return false;
        }

        return *version_ == *vve.version_;
    }

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<VersionedVariableExpression>(var(), version_->clone().release());
    }

private:
    void writeTo(Encoder&) const override {
        LOG(FATAL) << "VersionedVairableExpression not support to encode.";
    }

    void resetFrom(Decoder&) override {
        LOG(FATAL) << "VersionedVairableExpression not support to decode.";
    }

    std::string var_;
    // 0 means the latest, -1 the previous one, and so on.
    // 1 means the eldest, 2 the second elder one, and so on.
    std::unique_ptr<Expression> version_;
};
}  // namespace nebula
#endif
