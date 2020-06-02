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
                    std::unique_ptr<Expression>&& operand = nullptr)
        : Expression(kind)
        , operand_(std::move(operand)) {}

    bool operator==(const Expression& rhs) const override;

    Value eval(const ExpressionContext& ctx) const override;

    std::string toString() const override {
        // TODO
        return "";
    }

protected:
    std::unique_ptr<Expression> operand_;

    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;
};

}  // namespace nebula
#endif  // EXPRESSION_UNARYEXPRESSION_H_
