/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_PROPERTYEXPRESSION_H_
#define COMMON_EXPRESSION_PROPERTYEXPRESSION_H_

#include <cstddef>

#include "common/expression/Expression.h"

namespace nebula {

constexpr char const kInputRef[] = "$-";
constexpr char const kVarRef[] = "$";
constexpr char const kSrcRef[] = "$^";
constexpr char const kDstRef[] = "$$";

// Base abstract expression of getting properties.
// An expression of getting props is consisted with 3 parts:
// 1. reference, e.g. $-, $, $^, $$
// 2. symbol, a symbol name, e.g. tag_name, edge_name, variable_name,
// 3. property, property name.
//
// The PropertyExpression will only be used in parser to indicate
// the form of symbol.prop, it will be transform to a proper expression
// in a parse rule.
class PropertyExpression : public Expression {
  friend class Expression;

 public:
  bool operator==(const Expression& rhs) const override;

  bool isPropertyExpr() const override {
    return true;
  }

  const Value& eval(ExpressionContext& ctx) override;

  const std::string& ref() const {
    return ref_;
  }

  const std::string& sym() const {
    return sym_;
  }

  const std::string& prop() const {
    return prop_;
  }

  std::string toString() const override;

 protected:
  PropertyExpression(ObjectPool* pool,
                     Kind kind,
                     const std::string& ref,
                     const std::string& sym,
                     const std::string& prop)
      : Expression(pool, kind), ref_(ref), sym_(sym), prop_(prop) {}

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

  std::string ref_;
  std::string sym_;
  std::string prop_;
};

// edge_name.any_prop_name
class EdgePropertyExpression final : public PropertyExpression {
 public:
  EdgePropertyExpression& operator=(const EdgePropertyExpression& rhs) = delete;
  EdgePropertyExpression& operator=(EdgePropertyExpression&&) = delete;

  static EdgePropertyExpression* make(ObjectPool* pool,
                                      const std::string& edge = "",
                                      const std::string& prop = "") {
    return pool->makeAndAdd<EdgePropertyExpression>(pool, edge, prop);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return EdgePropertyExpression::make(pool_, sym(), prop());
  }

 private:
  friend ObjectPool;
  explicit EdgePropertyExpression(ObjectPool* pool,
                                  const std::string& edge = "",
                                  const std::string& prop = "")
      : PropertyExpression(pool, Kind::kEdgeProperty, "", edge, prop) {}

 private:
  Value result_;
};

// tag_name.any_prop_name
class TagPropertyExpression final : public PropertyExpression {
 public:
  TagPropertyExpression& operator=(const TagPropertyExpression& rhs) = delete;
  TagPropertyExpression& operator=(TagPropertyExpression&&) = delete;

  static TagPropertyExpression* make(ObjectPool* pool,
                                     const std::string& tag = "",
                                     const std::string& prop = "") {
    return pool->makeAndAdd<TagPropertyExpression>(pool, tag, prop);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return TagPropertyExpression::make(pool_, sym(), prop());
  }

 private:
  friend ObjectPool;
  explicit TagPropertyExpression(ObjectPool* pool,
                                 const std::string& tag = "",
                                 const std::string& prop = "")
      : PropertyExpression(pool, Kind::kTagProperty, "", tag, prop) {}

 private:
  Value result_;
};

// label.tag_name.any_prop_name
class LabelTagPropertyExpression final : public PropertyExpression {
 public:
  LabelTagPropertyExpression& operator=(const LabelTagPropertyExpression& rhs) = delete;
  LabelTagPropertyExpression& operator=(LabelTagPropertyExpression&&) = delete;

  static LabelTagPropertyExpression* make(ObjectPool* pool,
                                          Expression* label = nullptr,
                                          const std::string& tag = "",
                                          const std::string& prop = "") {
    return pool->makeAndAdd<LabelTagPropertyExpression>(pool, label, tag, prop);
  }

  std::string toString() const override;

  bool operator==(const Expression& rhs) const override;

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return LabelTagPropertyExpression::make(pool_, label_, sym(), prop());
  }

  const Expression* label() const {
    return label_;
  }

  Expression* label() {
    return label_;
  }

  void setLabel(Expression* label) {
    label_ = label;
  }

 private:
  friend ObjectPool;
  explicit LabelTagPropertyExpression(ObjectPool* pool,
                                      Expression* label = nullptr,
                                      const std::string& tag = "",
                                      const std::string& prop = "")
      : PropertyExpression(pool, Kind::kLabelTagProperty, "", tag, prop), label_(label) {}

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

 private:
  Expression* label_{nullptr};
};

// $-.any_prop_name
class InputPropertyExpression final : public PropertyExpression {
 public:
  InputPropertyExpression& operator=(const InputPropertyExpression& rhs) = delete;
  InputPropertyExpression& operator=(InputPropertyExpression&&) = delete;

  static InputPropertyExpression* make(ObjectPool* pool, const std::string& prop = "") {
    return pool->makeAndAdd<InputPropertyExpression>(pool, prop);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return InputPropertyExpression::make(pool_, prop());
  }

 private:
  friend ObjectPool;
  explicit InputPropertyExpression(ObjectPool* pool, const std::string& prop = "")
      : PropertyExpression(pool, Kind::kInputProperty, kInputRef, "", prop) {}

  // runtime info
  std::optional<std::size_t> propIndex_;
};

// $VarName.any_prop_name
class VariablePropertyExpression final : public PropertyExpression {
 public:
  VariablePropertyExpression& operator=(const VariablePropertyExpression& rhs) = delete;
  VariablePropertyExpression& operator=(VariablePropertyExpression&&) = delete;

  static VariablePropertyExpression* make(ObjectPool* pool,
                                          const std::string& var = "",
                                          const std::string& prop = "") {
    return pool->makeAndAdd<VariablePropertyExpression>(pool, var, prop);
  }

  const Value& eval(ExpressionContext& ctx) override;

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return VariablePropertyExpression::make(pool_, sym(), prop());
  }

 private:
  friend ObjectPool;
  explicit VariablePropertyExpression(ObjectPool* pool,
                                      const std::string& var = "",
                                      const std::string& prop = "")
      : PropertyExpression(pool, Kind::kVarProperty, kVarRef, var, prop) {}

  // runtime info
  std::optional<std::size_t> propIndex_;
};

// $^.TagName.any_prop_name
class SourcePropertyExpression final : public PropertyExpression {
 public:
  SourcePropertyExpression& operator=(const SourcePropertyExpression& rhs) = delete;
  SourcePropertyExpression& operator=(SourcePropertyExpression&&) = delete;

  static SourcePropertyExpression* make(ObjectPool* pool,
                                        const std::string& tag = "",
                                        const std::string& prop = "") {
    return pool->makeAndAdd<SourcePropertyExpression>(pool, tag, prop);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return SourcePropertyExpression::make(pool_, sym(), prop());
  }

 private:
  friend ObjectPool;
  explicit SourcePropertyExpression(ObjectPool* pool,
                                    const std::string& tag = "",
                                    const std::string& prop = "")
      : PropertyExpression(pool, Kind::kSrcProperty, kSrcRef, tag, prop) {}

 private:
  Value result_;
};

// $$.TagName.any_prop_name
class DestPropertyExpression final : public PropertyExpression {
 public:
  DestPropertyExpression& operator=(const DestPropertyExpression& rhs) = delete;
  DestPropertyExpression& operator=(DestPropertyExpression&&) = delete;

  static DestPropertyExpression* make(ObjectPool* pool,
                                      const std::string& tag = "",
                                      const std::string& prop = "") {
    return pool->makeAndAdd<DestPropertyExpression>(pool, tag, prop);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return DestPropertyExpression::make(pool_, sym(), prop());
  }

 private:
  friend ObjectPool;
  explicit DestPropertyExpression(ObjectPool* pool,
                                  const std::string& tag = "",
                                  const std::string& prop = "")
      : PropertyExpression(pool, Kind::kDstProperty, kDstRef, tag, prop) {}
};

// EdgeName._src
class EdgeSrcIdExpression final : public PropertyExpression {
 public:
  EdgeSrcIdExpression& operator=(const EdgeSrcIdExpression& rhs) = delete;
  EdgeSrcIdExpression& operator=(EdgeSrcIdExpression&&) = delete;

  static EdgeSrcIdExpression* make(ObjectPool* pool, const std::string& edge = "") {
    return pool->makeAndAdd<EdgeSrcIdExpression>(pool, edge);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return EdgeSrcIdExpression::make(pool_, sym());
  }

 private:
  friend ObjectPool;
  explicit EdgeSrcIdExpression(ObjectPool* pool, const std::string& edge = "")
      : PropertyExpression(pool, Kind::kEdgeSrc, "", edge, kSrc) {}

 private:
  Value result_;
};

// EdgeName._type
class EdgeTypeExpression final : public PropertyExpression {
 public:
  EdgeTypeExpression& operator=(const EdgeTypeExpression& rhs) = delete;
  EdgeTypeExpression& operator=(EdgeTypeExpression&&) = delete;

  static EdgeTypeExpression* make(ObjectPool* pool, const std::string& edge = "") {
    return pool->makeAndAdd<EdgeTypeExpression>(pool, edge);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return EdgeTypeExpression::make(pool_, sym());
  }

 private:
  friend ObjectPool;
  explicit EdgeTypeExpression(ObjectPool* pool, const std::string& edge = "")
      : PropertyExpression(pool, Kind::kEdgeType, "", edge, kType) {}

 private:
  Value result_;
};

// EdgeName._rank
class EdgeRankExpression final : public PropertyExpression {
 public:
  EdgeRankExpression& operator=(const EdgeRankExpression& rhs) = delete;
  EdgeRankExpression& operator=(EdgeRankExpression&&) = delete;

  static EdgeRankExpression* make(ObjectPool* pool, const std::string& edge = "") {
    return pool->makeAndAdd<EdgeRankExpression>(pool, edge);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return EdgeRankExpression::make(pool_, sym());
  }

 private:
  friend ObjectPool;
  explicit EdgeRankExpression(ObjectPool* pool, const std::string& edge = "")
      : PropertyExpression(pool, Kind::kEdgeRank, "", edge, kRank) {}

 private:
  Value result_;
};

// EdgeName._dst
class EdgeDstIdExpression final : public PropertyExpression {
 public:
  EdgeDstIdExpression& operator=(const EdgeDstIdExpression& rhs) = delete;
  EdgeDstIdExpression& operator=(EdgeDstIdExpression&&) = delete;

  static EdgeDstIdExpression* make(ObjectPool* pool, const std::string& edge = "") {
    return pool->makeAndAdd<EdgeDstIdExpression>(pool, edge);
  }

  const Value& eval(ExpressionContext& ctx) override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return EdgeDstIdExpression::make(pool_, sym());
  }

 private:
  friend ObjectPool;
  explicit EdgeDstIdExpression(ObjectPool* pool, const std::string& edge = "")
      : PropertyExpression(pool, Kind::kEdgeDst, "", edge, kDst) {}

 private:
  Value result_;
};

}  // namespace nebula
#endif
