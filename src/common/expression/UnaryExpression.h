/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_UNARYEXPRESSION_H_
#define COMMON_EXPRESSION_UNARYEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {

class UnaryExpression final : public Expression {
  friend class Expression;

 public:
  static UnaryExpression* makePlus(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kUnaryPlus, operand);
  }

  static UnaryExpression* makeNegate(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kUnaryNegate, operand);
  }

  static UnaryExpression* makeNot(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kUnaryNot, operand);
  }

  static UnaryExpression* makeIncr(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kUnaryIncr, operand);
  }

  static UnaryExpression* makeDecr(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kUnaryDecr, operand);
  }

  static UnaryExpression* makeIsNull(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kIsNull, operand);
  }

  static UnaryExpression* makeIsNotNull(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kIsNotNull, operand);
  }

  static UnaryExpression* makeIsEmpty(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kIsEmpty, operand);
  }

  static UnaryExpression* makeIsNotEmpty(ObjectPool* pool, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, Kind::kIsNotEmpty, operand);
  }

  static UnaryExpression* make(ObjectPool* pool, Kind kind, Expression* operand = nullptr) {
    return pool->makeAndAdd<UnaryExpression>(pool, kind, operand);
  }

  bool operator==(const Expression& rhs) const override;

  const Value& eval(ExpressionContext& ctx) override;

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return UnaryExpression::make(pool_, kind(), operand_->clone());
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

 private:
  friend ObjectPool;
  UnaryExpression(ObjectPool* pool, Kind kind, Expression* operand = nullptr)
      : Expression(pool, kind), operand_(operand) {}

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

 private:
  Expression* operand_;
  Value result_;
};

}  // namespace nebula
#endif  // EXPRESSION_UNARYEXPRESSION_H_
