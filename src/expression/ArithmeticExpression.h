/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXPRESSION_ARITHMETICEXPRESSION_H_
#define EXPRESSION_ARITHMETICEXPRESSION_H_

#include "expression/Expression.h"

namespace nebula {

class ArithmeticExpression : public Expression {
public:
    ArithmeticExpression(Type type,
                         std::unique_ptr<Expression> left,
                         std::unique_ptr<Expression> right)
        : type_(type)
        , left_(std::move(left))
        , right_(std::move(right)) {}

    Value eval() const override;

    Type type() const override {
        return type_;
    }

    std::string encode() const override;

private:
    Type type_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

}  // namespace nebula
#endif  // EXPRESSION_EXPRESSION_H_
