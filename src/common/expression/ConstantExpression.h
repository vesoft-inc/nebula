/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_CONSTANTEXPRESSION_H_
#define COMMON_EXPRESSION_CONSTANTEXPRESSION_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"

namespace nebula {

class ConstantExpression : public Expression {
    friend class Expression;

public:
    explicit ConstantExpression(Value v = Value(NullType::__NULL__))
        : Expression(Kind::kConstant)
        , val_(std::move(v)) {}

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override {
        UNUSED(ctx);
        return val_;
    }

    void accept(ExprVisitor* visitor) override;

    std::string toString() const override;

private:
    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder& decoder) override;

    Value val_;
};

}   // namespace nebula
#endif   // COMMON_EXPRESSION_CONSTANTEXPRESSION_H_
