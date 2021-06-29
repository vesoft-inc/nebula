/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_COLUMNEXPRESSION_H_
#define COMMON_EXPRESSION_COLUMNEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {
/**
 * This is an internal type of expression
 * It has no corresponding rules in parser.
 * you can get the corresponding value by column index
 */
class ColumnExpression final : public Expression {
public:
    ColumnExpression& operator=(const ColumnExpression& rhs) = delete;
    ColumnExpression& operator=(ColumnExpression&&) = delete;

    static ColumnExpression* make(ObjectPool* pool, int32_t index = 0) {
        return pool->add(new ColumnExpression(pool, index));
    }

    const Value& eval(ExpressionContext& ctx) override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        return ColumnExpression::make(pool_, index_);
    }

    std::string toString() const override;

    bool operator==(const Expression& expr) const override;

private:
    explicit ColumnExpression(ObjectPool* pool, int32_t index = 0)
        : Expression(pool, Kind::kColumn), index_(index) {}

    void writeTo(Encoder& encoder) const override;

    void resetFrom(Decoder&) override;

private:
    Value                                   result_;
    int32_t                                 index_;
};

}   // namespace nebula

#endif  // COMMON_EXPRESSION_COLUMNEXPRESSION_H_
