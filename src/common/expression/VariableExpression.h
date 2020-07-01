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
    explicit VariableExpression(std::string* var)
        : Expression(Kind::kVar) {
        var_.reset(var);
    }

    const std::string& var() const {
        return *var_.get();
    }

    const Value& eval(ExpressionContext& ctx) override;

    bool operator==(const Expression& rhs) const override {
        UNUSED(rhs);
        return false;
    }

    std::string toString() const override;

private:
    void writeTo(Encoder& encoder) const override {
        UNUSED(encoder);
    }

    void resetFrom(Decoder& decoder) override {
        UNUSED(decoder);
    }

    std::unique_ptr<std::string>                 var_;
};

/*
 * VersionedVariableExpression is designed for getting the historical results
 * of a variable.
 */
class VersionedVariableExpression final : public Expression {
public:
    VersionedVariableExpression(std::string* var, Expression* version)
        : Expression(Kind::kVersionedVar) {
        var_.reset(var);
        version_.reset(version);
    }

    const std::string& var() const {
        return *var_.get();
    }

    const Value& eval(ExpressionContext& ctx) override;

    bool operator==(const Expression& rhs) const override {
        UNUSED(rhs);
        return false;
    }

    std::string toString() const override;

private:
    void writeTo(Encoder& encoder) const override {
        UNUSED(encoder);
    }

    void resetFrom(Decoder& decoder) override {
        UNUSED(decoder);
    }

    std::unique_ptr<std::string>                 var_;
    // 0 means the latest, -1 the previous one, and so on.
    // 1 means the eldest, 2 the second elder one, and so on.
    std::unique_ptr<Expression>                  version_;
};
}  // namespace nebula
#endif
