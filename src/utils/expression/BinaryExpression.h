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
    BinaryExpression(Kind kind,
                     std::unique_ptr<Expression>&& lhs,
                     std::unique_ptr<Expression>&& rhs)
        : Expression(kind)
        , lhs_(std::move(lhs))
        , rhs_(std::move(rhs)) {}

    bool operator==(const Expression& rhs) const override;

protected:
    std::unique_ptr<Expression>                 lhs_;
    std::unique_ptr<Expression>                 rhs_;

    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;
};

}  // namespace nebula
#endif  // COMMON_EXPRESSION_BINARYEXPRESSION_H_

