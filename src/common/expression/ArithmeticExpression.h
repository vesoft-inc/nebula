/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_ARITHMETICEXPRESSION_H_
#define COMMON_EXPRESSION_ARITHMETICEXPRESSION_H_

#include "common/expression/BinaryExpression.h"

namespace nebula {

class ArithmeticExpression final : public BinaryExpression {
 public:
  ArithmeticExpression& operator=(const ArithmeticExpression& rhs) = delete;
  ArithmeticExpression& operator=(ArithmeticExpression&&) = delete;

  static ArithmeticExpression* makeAdd(ObjectPool* pool,
                                       Expression* lhs = nullptr,
                                       Expression* rhs = nullptr) {
    return pool->makeAndAdd<ArithmeticExpression>(pool, Expression::Kind::kAdd, lhs, rhs);
  }
  static ArithmeticExpression* makeMinus(ObjectPool* pool,
                                         Expression* lhs = nullptr,
                                         Expression* rhs = nullptr) {
    return pool->makeAndAdd<ArithmeticExpression>(pool, Expression::Kind::kMinus, lhs, rhs);
  }
  static ArithmeticExpression* makeMultiply(ObjectPool* pool,
                                            Expression* lhs = nullptr,
                                            Expression* rhs = nullptr) {
    return pool->makeAndAdd<ArithmeticExpression>(pool, Expression::Kind::kMultiply, lhs, rhs);
  }
  static ArithmeticExpression* makeDivision(ObjectPool* pool,
                                            Expression* lhs = nullptr,
                                            Expression* rhs = nullptr) {
    return pool->makeAndAdd<ArithmeticExpression>(pool, Expression::Kind::kDivision, lhs, rhs);
  }
  static ArithmeticExpression* makeMod(ObjectPool* pool,
                                       Expression* lhs = nullptr,
                                       Expression* rhs = nullptr) {
    return pool->makeAndAdd<ArithmeticExpression>(pool, Expression::Kind::kMod, lhs, rhs);
  }
  // Construct arithmetic expression with given kind
  static ArithmeticExpression* makeKind(ObjectPool* pool,
                                        Kind kind,
                                        Expression* lhs = nullptr,
                                        Expression* rhs = nullptr) {
    return pool->makeAndAdd<ArithmeticExpression>(pool, kind, lhs, rhs);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  std::string toString() const override;

  Expression* clone() const override {
    return pool_->makeAndAdd<ArithmeticExpression>(
        pool_, kind(), left()->clone(), right()->clone());
  }

  bool isArithmeticExpr() const override {
    return true;
  }

 private:
  friend ObjectPool;
  ArithmeticExpression(ObjectPool* pool, Kind kind, Expression* lhs, Expression* rhs)
      : BinaryExpression(pool, kind, lhs, rhs) {}

 private:
  Value result_;
};

}  // namespace nebula
#endif  // COMMON_EXPRESSION_EXPRESSION_H_
