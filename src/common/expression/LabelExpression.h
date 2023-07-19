/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_LABELEXPRESSION_H_
#define COMMON_EXPRESSION_LABELEXPRESSION_H_

#include "common/expression/Expression.h"

// The LabelExpression use for name_label in base_expression,
// need to rewrite it based on the usage scenario
namespace nebula {

class LabelExpression : public Expression {
 public:
  LabelExpression& operator=(const LabelExpression& rhs) = delete;
  LabelExpression& operator=(LabelExpression&&) = delete;

  static LabelExpression* make(ObjectPool* pool, const std::string& name = "") {
    return pool->makeAndAdd<LabelExpression>(pool, name);
  }

  bool operator==(const Expression& rhs) const override;

  const Value& eval(ExpressionContext& ctx) override;

  const std::string& name() const {
    return name_;
  }

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return LabelExpression::make(pool_, name());
  }

 protected:
  friend ObjectPool;
  explicit LabelExpression(ObjectPool* pool, const std::string& name = "")
      : Expression(pool, Kind::kLabel), name_(name) {}

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

 protected:
  std::string name_;
  Value result_;
};

}  // namespace nebula
#endif  // COMMON_EXPRESSION_LABELEXPRESSION_H_
