/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/expression/Expression.h"

// The ParameterExpression use for parameterized statement
namespace nebula {

class ParameterExpression : public Expression {
 public:
  ParameterExpression& operator=(const ParameterExpression& rhs) = delete;
  ParameterExpression& operator=(ParameterExpression&&) = delete;

  static ParameterExpression* make(ObjectPool* pool, const std::string& name = "") {
    return pool->add(new ParameterExpression(pool, name));
  }

  bool operator==(const Expression& rhs) const override;

  const Value& eval(ExpressionContext& ctx) override;

  const std::string& name() const { return name_; }

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override { return ParameterExpression::make(pool_, name()); }

 protected:
  explicit ParameterExpression(ObjectPool* pool, const std::string& name = "")
      : Expression(pool, Kind::kParam), name_(name) {}

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

 protected:
  std::string name_;
  Value result_;
};

}  // namespace nebula
