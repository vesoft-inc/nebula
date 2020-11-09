/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_LOGICALEXPRESSION_H_
#define COMMON_EXPRESSION_LOGICALEXPRESSION_H_

#include "common/expression/BinaryExpression.h"

namespace nebula {
class LogicalExpression final : public Expression {
public:
    explicit LogicalExpression(Kind kind) : Expression(kind) {}

    LogicalExpression(Kind kind, Expression* lhs, Expression* rhs)
        : Expression(kind) {
        operands_.emplace_back(lhs);
        operands_.emplace_back(rhs);
    }

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    std::unique_ptr<Expression> clone() const override {
        auto copy = std::make_unique<LogicalExpression>(kind());
        copy->operands_.resize(operands_.size());
        for (auto i = 0u; i < operands_.size(); i++) {
            copy->operands_[i] = operands_[i]->clone();
        }
        return copy;
    }

    bool operator==(const Expression &rhs) const override;

    auto& operands() {
        return operands_;
    }

    const auto& operands() const {
        return operands_;
    }

    auto* operand(size_t index) {
        return operands_[index].get();
    }

    const auto* operand(size_t index) const {
        return operands_[index].get();
    }

    void addOperand(Expression *expr) {
        operands_.emplace_back(expr);
    }

    void setOperand(size_t i, Expression *operand) {
        DCHECK_LT(i, operands_.size());
        operands_[i].reset(operand);
    }

private:
    void writeTo(Encoder &encoder) const override;
    void resetFrom(Decoder &decoder) override;
    const Value& evalAnd(ExpressionContext &ctx);
    const Value& evalOr(ExpressionContext &ctx);
    const Value& evalXor(ExpressionContext &ctx);

private:
    Value                                       result_;
    std::vector<std::unique_ptr<Expression>>    operands_;
};

}   // namespace nebula
#endif  // COMMON_EXPRESSION_LOGICALEXPRESSION_H_
