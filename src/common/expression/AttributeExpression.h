/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_ATTRIBUTEEXPRESSION_H_
#define COMMON_EXPRESSION_ATTRIBUTEEXPRESSION_H_

#include "common/expression/BinaryExpression.h"

namespace nebula {

// <expr>.label
class AttributeExpression final : public BinaryExpression {
 public:
  AttributeExpression &operator=(const AttributeExpression &rhs) = delete;
  AttributeExpression &operator=(AttributeExpression &&) = delete;

  static AttributeExpression *make(ObjectPool *pool,
                                   Expression *lhs = nullptr,
                                   Expression *rhs = nullptr) {
    return pool->makeAndAdd<AttributeExpression>(pool, lhs, rhs);
  }

  const Value &eval(ExpressionContext &ctx) override;

  void accept(ExprVisitor *visitor) override;

  std::string toString() const override;

  Expression *clone() const override {
    return AttributeExpression::make(pool_, left()->clone(), right()->clone());
  }

 private:
  friend ObjectPool;
  explicit AttributeExpression(ObjectPool *pool,
                               Expression *lhs = nullptr,
                               Expression *rhs = nullptr)
      : BinaryExpression(pool, Kind::kAttribute, lhs, rhs) {}

 private:
  Value result_;
};

}  // namespace nebula

#endif  // COMMON_EXPRESSION_ATTRIBUTEEXPRESSION_H_
