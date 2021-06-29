/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_UUIDEXPRESSION_H_
#define COMMON_EXPRESSION_UUIDEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class UUIDExpression final : public Expression {
    friend class Expression;

public:
    static UUIDExpression* make(ObjectPool* pool, const std::string& field = "") {
        DCHECK(!!pool);
        return pool->add(new UUIDExpression(pool, field));
    }

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        return UUIDExpression::make(pool_, field_);
    }

private:
    explicit UUIDExpression(ObjectPool* pool, const std::string& field = "")
        : Expression(pool, Kind::kUUID), field_(field) {}

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

private:
    std::string field_;
    Value result_;
};

}   // namespace nebula
#endif  // EXPRESSION_UUIDEXPRESSION_H_
