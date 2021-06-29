/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_BINARYEXPRESSION_H_
#define COMMON_EXPRESSION_BINARYEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

/**
 *  Base class for all binary expressions
 **/
class BinaryExpression : public Expression {
    friend class Expression;

public:
    bool operator==(const Expression& rhs) const override;

    const Expression* left() const {
        return lhs_;
    }

    Expression* left() {
        return lhs_;
    }

    void setLeft(Expression* expr) {
        lhs_ = expr;
    }

    const Expression* right() const {
        return rhs_;
    }

    Expression* right() {
        return rhs_;
    }

    void setRight(Expression* expr) {
        rhs_ = expr;
    }

protected:
    BinaryExpression(ObjectPool* pool, Kind kind, Expression* lhs, Expression* rhs)
        : Expression(pool, kind), lhs_(lhs), rhs_(rhs) {}

    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

protected:
    Expression* lhs_ = nullptr;
    Expression* rhs_ = nullptr;
};

}  // namespace nebula
#endif  // COMMON_EXPRESSION_BINARYEXPRESSION_H_
