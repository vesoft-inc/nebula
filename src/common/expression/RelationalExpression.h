/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_RELATIONALEXPRESSION_H_
#define COMMON_EXPRESSION_RELATIONALEXPRESSION_H_

#include "common/expression/BinaryExpression.h"

namespace nebula {
class RelationalExpression final : public BinaryExpression {
 public:
  static RelationalExpression* makeEQ(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelEQ, lhs, rhs);
  }

  static RelationalExpression* makeNE(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelNE, lhs, rhs);
  }

  static RelationalExpression* makeLT(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelLT, lhs, rhs);
  }

  static RelationalExpression* makeLE(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelLE, lhs, rhs);
  }

  static RelationalExpression* makeGT(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelGT, lhs, rhs);
  }

  static RelationalExpression* makeGE(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelGE, lhs, rhs);
  }

  static RelationalExpression* makeREG(ObjectPool* pool,
                                       Expression* lhs = nullptr,
                                       Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelREG, lhs, rhs);
  }

  static RelationalExpression* makeIn(ObjectPool* pool,
                                      Expression* lhs = nullptr,
                                      Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelIn, lhs, rhs);
  }

  static RelationalExpression* makeNotIn(ObjectPool* pool,
                                         Expression* lhs = nullptr,
                                         Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kRelNotIn, lhs, rhs);
  }

  static RelationalExpression* makeContains(ObjectPool* pool,
                                            Expression* lhs = nullptr,
                                            Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kContains, lhs, rhs);
  }

  static RelationalExpression* makeNotContains(ObjectPool* pool,
                                               Expression* lhs = nullptr,
                                               Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kNotContains, lhs, rhs);
  }

  static RelationalExpression* makeStartsWith(ObjectPool* pool,
                                              Expression* lhs = nullptr,
                                              Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kStartsWith, lhs, rhs);
  }

  static RelationalExpression* makeNotStartsWith(ObjectPool* pool,
                                                 Expression* lhs = nullptr,
                                                 Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kNotStartsWith, lhs, rhs);
  }

  static RelationalExpression* makeEndsWith(ObjectPool* pool,
                                            Expression* lhs = nullptr,
                                            Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kEndsWith, lhs, rhs);
  }

  static RelationalExpression* makeNotEndsWith(ObjectPool* pool,
                                               Expression* lhs = nullptr,
                                               Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, Kind::kNotEndsWith, lhs, rhs);
  }

  // Construct a kind-specified relational expression
  static RelationalExpression* makeKind(ObjectPool* pool,
                                        Kind kind,
                                        Expression* lhs = nullptr,
                                        Expression* rhs = nullptr) {
    return pool->makeAndAdd<RelationalExpression>(pool, kind, lhs, rhs);
  }

  const Value& eval(ExpressionContext& ctx) override;

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return RelationalExpression::makeKind(pool_, kind(), left()->clone(), right()->clone());
  }

  bool isRelExpr() const override {
    return true;
  }

 private:
  friend ObjectPool;
  RelationalExpression(ObjectPool* pool, Kind kind, Expression* lhs, Expression* rhs)
      : BinaryExpression(pool, kind, lhs, rhs) {}

 private:
  Value result_;
};

}  // namespace nebula
#endif  // COMMON_EXPRESSION_RELATIONALEXPRESSION_H_
