/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_ATTRIBUTEEXPRESSION_H_
#define COMMON_EXPRESSION_ATTRIBUTEEXPRESSION_H_

#include <string>  // for operator<<, string

#include "common/base/ObjectPool.h"              // for ObjectPool
#include "common/datatypes/Value.h"              // for Value
#include "common/expression/BinaryExpression.h"  // for BinaryExpression
#include "common/expression/Expression.h"        // for Expression, Expressi...

namespace nebula {
class ExprVisitor;
class ExpressionContext;

class ExprVisitor;
class ExpressionContext;

// <expr>.label
class AttributeExpression final : public BinaryExpression {
 public:
  AttributeExpression &operator=(const AttributeExpression &rhs) = delete;
  AttributeExpression &operator=(AttributeExpression &&) = delete;

  static AttributeExpression *make(ObjectPool *pool,
                                   Expression *lhs = nullptr,
                                   Expression *rhs = nullptr) {
    return pool->add(new AttributeExpression(pool, lhs, rhs));
  }

  const Value &eval(ExpressionContext &ctx) override;

  void accept(ExprVisitor *visitor) override;

  std::string toString() const override;

  Expression *clone() const override {
    return AttributeExpression::make(pool_, left()->clone(), right()->clone());
  }

 private:
  explicit AttributeExpression(ObjectPool *pool,
                               Expression *lhs = nullptr,
                               Expression *rhs = nullptr)
      : BinaryExpression(pool, Kind::kAttribute, lhs, rhs) {}

 private:
  Value result_;
};

}  // namespace nebula

#endif  // COMMON_EXPRESSION_ATTRIBUTEEXPRESSION_H_
