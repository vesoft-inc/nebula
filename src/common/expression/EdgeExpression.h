/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_EDGEEXPRESSION_H_
#define COMMON_EXPRESSION_EDGEEXPRESSION_H_

#include <string>  // for allocator, operator<<, string

#include "common/base/ObjectPool.h"        // for ObjectPool
#include "common/datatypes/Value.h"        // for Value
#include "common/expression/Expression.h"  // for Expression, Expression::En...

namespace nebula {
class ExprVisitor;
class ExpressionContext;

class ExprVisitor;
class ExpressionContext;

/**
 * This is an internal type of expression to denote a Edge value.
 * It has no corresponding rules in parser.
 * It is generated from LabelExpression during semantic analysis
 * and expression rewrite.
 */
class EdgeExpression final : public Expression {
 public:
  EdgeExpression& operator=(const EdgeExpression& rhs) = delete;
  EdgeExpression& operator=(EdgeExpression&&) = delete;

  static EdgeExpression* make(ObjectPool* pool) {
    return pool->add(new EdgeExpression(pool));
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return EdgeExpression::make(pool_);
  }

  std::string toString() const override {
    return "EDGE";
  }

  bool operator==(const Expression& expr) const override {
    return kind() == expr.kind();
  }

 private:
  explicit EdgeExpression(ObjectPool* pool) : Expression(pool, Kind::kEdge) {}

  void writeTo(Encoder& encoder) const override {
    encoder << kind();
  }

  void resetFrom(Decoder&) override {}

 private:
  Value result_;
};

}  // namespace nebula

#endif  // COMMON_EXPRESSION_EDGEEXPRESSION_H_
