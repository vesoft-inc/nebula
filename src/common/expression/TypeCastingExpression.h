/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_TYPECASTINGEXPRESSION_H_
#define COMMON_EXPRESSION_TYPECASTINGEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class TypeCastingExpression final : public Expression {
    friend class Expression;

public:
    static TypeCastingExpression* make(ObjectPool* pool,
                                       Value::Type vType = Value::Type::__EMPTY__,
                                       Expression* operand = nullptr) {
        DCHECK(!!pool);
        return pool->add(new TypeCastingExpression(pool, vType, operand));
    }

    bool operator==(const Expression& rhs) const override;

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        return TypeCastingExpression::make(pool_, type(), operand()->clone());
    }

    const Expression* operand() const {
        return operand_;
    }

    Expression* operand() {
        return operand_;
    }

    void setOperand(Expression* expr) {
        operand_ = expr;
    }

    Value::Type type() const {
        return vType_;
    }

    static bool validateTypeCast(Value::Type operandType, Value::Type type);

private:
    explicit TypeCastingExpression(ObjectPool* pool,
                                   Value::Type vType = Value::Type::__EMPTY__,
                                   Expression* operand = nullptr)
        : Expression(pool, Kind::kTypeCasting), vType_(vType), operand_(operand) {}

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;

private:
    Value::Type vType_{Value::Type::__EMPTY__};
    Expression* operand_;
    Value result_;
};

}  // namespace nebula
#endif  // EXPRESSION_TYPECASTINGEXPRESSION_H_
