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
    ConstantExpression& operator=(const ConstantExpression& rhs) = delete;
    ConstantExpression& operator=(ConstantExpression&&) = delete;

    static ConstantExpression* make(ObjectPool* pool, Value v = Value(NullType::__NULL__)) {
        DCHECK(!!pool);
        return pool->add(new ConstantExpression(pool, v));
    }

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override {
        UNUSED(ctx);
        return val_;
    }

    const Value& value() const {
        return val_;
    }

    void setValue(Value val) {
        val_ = std::move(val);
    }

    void accept(ExprVisitor* visitor) override;

    std::string toString() const override;

    Expression* clone() const override {
        return ConstantExpression::make(pool_, val_);
    }

private:
    explicit ConstantExpression(ObjectPool* pool, Value v = Value(NullType::__NULL__))
        : Expression(pool, Kind::kConstant), val_(std::move(v)) {}

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

private:
    Value val_;
};

}   // namespace nebula
#endif   // COMMON_EXPRESSION_CONSTANTEXPRESSION_H_
