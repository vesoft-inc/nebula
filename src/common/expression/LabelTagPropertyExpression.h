/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/expression/Expression.h"

/**
 * This is an internal type of expression to denote a var.tagname.propname value.
 * It has no corresponding rules in parser.
 * It is generated from AttributeExpression during semantic analysis
 * and expression rewrite.
 */
namespace nebula {

class LabelTagPropertyExpression final : public Expression {
 public:
  LabelTagPropertyExpression& operator=(const LabelTagPropertyExpression& rhs) = delete;
  LabelTagPropertyExpression& operator=(LabelTagPropertyExpression&&) = delete;

  static LabelTagPropertyExpression* make(ObjectPool* pool,
                                          const std::string& label = "",
                                          const std::string& tag = "",
                                          const std::string& prop = "") {
    return pool->add(new LabelTagPropertyExpression(pool, label, tag, prop));
  }

  std::string toString() const override;

  bool operator==(const Expression& rhs) const override;

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return LabelTagPropertyExpression::make(pool_, label_, tag_, prop_);
  }

  const std::string& prop() { return prop_; }

  const std::string& tag() { return tag_; }

  const std::string& label() { return label_; }

 private:
  LabelTagPropertyExpression(ObjectPool* pool,
                             const std::string& label = "",
                             const std::string& tag = "",
                             const std::string& prop = "")
      : Expression(pool, Kind::kLabelTagProperty), label_(label), tag_(tag), prop_(prop) {}

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

 private:
  std::string label_;
  std::string tag_;
  std::string prop_;
};

}  // namespace nebula
