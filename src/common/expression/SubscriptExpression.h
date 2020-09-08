/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_SUBSCRIPTEXPRESSION_H_
#define COMMON_EXPRESSION_SUBSCRIPTEXPRESSION_H_

#include "common/expression/BinaryExpression.h"

namespace nebula {

class SubscriptExpression final : public BinaryExpression {
public:
    explicit SubscriptExpression(Expression *lhs = nullptr,
                                 Expression *rhs = nullptr)
        : BinaryExpression(Kind::kSubscript, lhs, rhs) {}

    const Value& eval(ExpressionContext &ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        return std::make_unique<SubscriptExpression>(left()->clone().release(),
                                                     right()->clone().release());
    }

private:
    Value                               result_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_SUBSCRIPTEXPRESSION_H_
