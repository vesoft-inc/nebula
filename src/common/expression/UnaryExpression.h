/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_UNARYEXPRESSION_H_
#define COMMON_EXPRESSION_UNARYEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class UnaryExpression final : public Expression {
    friend class Expression;

public:
    UnaryExpression(Kind kind,
                    Expression* operand = nullptr)
        : Expression(kind)
        , operand_(std::move(operand)) {}

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<UnaryExpression>(kind(), operand()->clone().release());
    }

    const Expression* operand() const {
        return operand_.get();
    }

    Expression* operand() {
        return operand_.get();
    }

    void setOperand(Expression* expr) {
        operand_.reset(expr);
    }

private:
    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

    std::unique_ptr<Expression> operand_;
    Value                       result_;
};

}  // namespace nebula
#endif  // EXPRESSION_UNARYEXPRESSION_H_
