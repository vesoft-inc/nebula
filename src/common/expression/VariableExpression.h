/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_VARIABLEEXPRESSION_H_
#define COMMON_EXPRESSION_VARIABLEEXPRESSION_H_

#include "common/expression/Expression.h"

namespace nebula {
class VariableExpression final : public Expression {
 public:
  // Make a non-inner variable expression
  static VariableExpression* make(ObjectPool* pool, const std::string& var = "") {
    return pool->makeAndAdd<VariableExpression>(pool, var, false);
  }

  // Make a inner variable expression. Inner variable is a variable defined in an expression.
  // e.g. ListComprehensionExpression [i IN range(1, 10) | i+1]
  static VariableExpression* makeInner(ObjectPool* pool, const std::string& var = "") {
    return pool->makeAndAdd<VariableExpression>(pool, var, true);
  }

  const std::string& var() const {
    return var_;
  }

  bool isInner() const {
    return isInner_;
  }

  void setInner(bool inner) {
    isInner_ = inner;
  }

  const Value& eval(ExpressionContext& ctx) override;

  bool operator==(const Expression& rhs) const override {
    if (kind() != rhs.kind()) {
      return false;
    }
    return var() == static_cast<const VariableExpression&>(rhs).var();
  }

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return pool_->makeAndAdd<VariableExpression>(pool_, var(), isInner_);
  }

 private:
  friend ObjectPool;
  explicit VariableExpression(ObjectPool* pool, const std::string& var = "", bool isInner = false)
      : Expression(pool, Kind::kVar), isInner_(isInner), var_(var) {}

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

 private:
  bool isInner_{false};
  std::string var_;
};

/*
 * VersionedVariableExpression is designed for getting the historical results
 * of a variable.
 */
class VersionedVariableExpression final : public Expression {
 public:
  static VersionedVariableExpression* make(ObjectPool* pool,
                                           const std::string& var = "",
                                           Expression* version = nullptr) {
    return pool->makeAndAdd<VersionedVariableExpression>(pool, var, version);
  }

  const std::string& var() const {
    return var_;
  }

  const Value& eval(ExpressionContext& ctx) override;

  bool operator==(const Expression& rhs) const override {
    if (kind() != rhs.kind()) {
      return false;
    }

    auto& vve = static_cast<const VersionedVariableExpression&>(rhs);
    if (var() != vve.var()) {
      return false;
    }

    return *version_ == *vve.version_;
  }

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override {
    return VersionedVariableExpression::make(pool_, var(), version_->clone());
  }

 private:
  friend ObjectPool;
  explicit VersionedVariableExpression(ObjectPool* pool,
                                       const std::string& var = "",
                                       Expression* version = nullptr)
      : Expression(pool, Kind::kVersionedVar), var_(var), version_(version) {}

  void writeTo(Encoder&) const override {
    LOG(FATAL) << "VersionedVariableExpression not support to encode.";
  }

  void resetFrom(Decoder&) override {
    LOG(FATAL) << "VersionedVariableExpression not support to decode.";
  }

 private:
  std::string var_;
  // 0 means the latest, -1 the previous one, and so on.
  // 1 means the eldest, 2 the second elder one, and so on.
  Expression* version_{nullptr};
};
}  // namespace nebula
#endif
