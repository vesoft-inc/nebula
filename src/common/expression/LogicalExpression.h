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
    LogicalExpression& operator=(const LogicalExpression& rhs) = delete;
    LogicalExpression& operator=(LogicalExpression&&) = delete;

    static LogicalExpression* makeAnd(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
        DCHECK(!!pool);
        return (lhs && rhs) ? pool->add(new LogicalExpression(pool, Kind::kLogicalAnd, lhs, rhs))
                            : pool->add(new LogicalExpression(pool, Kind::kLogicalAnd));
    }

    static LogicalExpression* makeOr(ObjectPool* pool,
                                     Expression* lhs = nullptr,
                                     Expression* rhs = nullptr) {
        DCHECK(!!pool);
        return (lhs && rhs) ? pool->add(new LogicalExpression(pool, Kind::kLogicalOr, lhs, rhs))
                            : pool->add(new LogicalExpression(pool, Kind::kLogicalOr));
    }

    static LogicalExpression* makeXor(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
        DCHECK(!!pool);
        return (lhs && rhs) ? pool->add(new LogicalExpression(pool, Kind::kLogicalXor, lhs, rhs))
                            : pool->add(new LogicalExpression(pool, Kind::kLogicalXor));
    }

    static LogicalExpression* makeKind(ObjectPool* pool,
                                       Kind kind,
                                       Expression* lhs = nullptr,
                                       Expression* rhs = nullptr) {
        DCHECK(!!pool);
        return (lhs && rhs) ? pool->add(new LogicalExpression(pool, kind, lhs, rhs))
                            : pool->add(new LogicalExpression(pool, kind));
    }

    const Value& eval(ExpressionContext& ctx) override;

    std::string toString() const override;

    void accept(ExprVisitor* visitor) override;

    Expression* clone() const override {
        auto copy = new LogicalExpression(pool_, kind());
        copy->operands_.resize(operands_.size());
        for (auto i = 0u; i < operands_.size(); i++) {
            copy->operands_[i] = operands_[i]->clone();
        }
        return pool_->add(copy);
    }

    bool operator==(const Expression &rhs) const override;

    auto& operands() {
        return operands_;
    }

    const auto& operands() const {
        return operands_;
    }

    auto* operand(size_t index) {
        return operands_[index];
    }

    const auto* operand(size_t index) const {
        return operands_[index];
    }

    void addOperand(Expression *expr) {
        operands_.emplace_back(expr);
    }

    void setOperand(size_t i, Expression *operand) {
        DCHECK_LT(i, operands_.size());
        operands_[i] = operand;
    }

    void setOperands(std::vector<Expression*> operands) {
        operands_ = operands;
    }

    bool isLogicalExpr() const override {
        return true;
    }

private:
    explicit LogicalExpression(ObjectPool* pool, Kind kind) : Expression(pool, kind) {}

    LogicalExpression(ObjectPool* pool, Kind kind, Expression* lhs, Expression* rhs)
        : Expression(pool, kind) {
        operands_.emplace_back(lhs);
        operands_.emplace_back(rhs);
    }

    void writeTo(Encoder& encoder) const override;
    void resetFrom(Decoder& decoder) override;
    const Value& evalAnd(ExpressionContext& ctx);
    const Value& evalOr(ExpressionContext& ctx);
    const Value& evalXor(ExpressionContext& ctx);

private:
    Value                       result_;
    std::vector<Expression*>    operands_;
};

}   // namespace nebula
#endif  // COMMON_EXPRESSION_LOGICALEXPRESSION_H_
