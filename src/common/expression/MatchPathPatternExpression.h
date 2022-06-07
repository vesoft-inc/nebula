/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_MATCHPATHPATTERNEXPRESSION_H_
#define COMMON_EXPRESSION_MATCHPATHPATTERNEXPRESSION_H_

#include "common/expression/Expression.h"
#include "parser/MatchSentence.h"

namespace nebula {

// For expression like (v:player)-[e:like]->(p)
// Evaluate to [[v, e, p], [v, e, p]...]
class MatchPathPatternExpression final : public Expression {
 public:
  MatchPathPatternExpression& operator=(const MatchPathPatternExpression& rhs) = delete;
  MatchPathPatternExpression& operator=(MatchPathPatternExpression&&) = delete;

  static MatchPathPatternExpression* make(ObjectPool* pool,
                                          std::unique_ptr<MatchPath>&& matchPath) {
    return pool->makeAndAdd<MatchPathPatternExpression>(pool, std::move(matchPath));
  }

  const Value& eval(ExpressionContext& ctx) override;

  bool operator==(const Expression& rhs) const override;

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override;

  // Evaluate expression by fetch result from input variable
  void setInputProp(const std::string& prop) {
    prop_ = InputPropertyExpression::make(pool_, prop);
  }

  void setInputProp(InputPropertyExpression* expr) {
    prop_ = expr;
  }

  InputPropertyExpression* inputProp() const {
    return prop_;
  }

  const MatchPath& matchPath() const {
    return *matchPath_;
  }

  MatchPath& matchPath() {
    return *matchPath_;
  }

 private:
  friend ObjectPool;
  explicit MatchPathPatternExpression(ObjectPool* pool, std::unique_ptr<MatchPath>&& matchPath)
      : Expression(pool, Kind::kMatchPathPattern), matchPath_(std::move(matchPath)) {}

  // This expression contains variable implicitly, so we don't support persist or transform it.
  void writeTo(Encoder&) const override {
    LOG(FATAL) << "Not implemented";
  }

  // This expression contains variable implicitly, so we don't support persist or transform it.
  void resetFrom(Decoder&) override {
    LOG(FATAL) << "Not implemented";
  }

 private:
  std::unique_ptr<MatchPath> matchPath_;
  InputPropertyExpression* prop_{
      nullptr};  // The column of input stored the result of the expression
  Value result_;
};
}  // namespace nebula
#endif  // COMMON_EXPRESSION_MATCHPATHPATTERNEXPRESSION_H_
