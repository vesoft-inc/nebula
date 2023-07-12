/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_TEXTSEARCHEXPRESSION_H_
#define COMMON_EXPRESSION_TEXTSEARCHEXPRESSION_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"

namespace nebula {

class TextSearchArgument final {
 public:
  static TextSearchArgument* make(ObjectPool* pool,
                                  const std::string& index,
                                  const std::string& query) {
    return pool->makeAndAdd<TextSearchArgument>(index, query);
  }

  ~TextSearchArgument() = default;

  std::string& index() {
    return index_;
  }

  std::string& query() {
    return query_;
  }

  bool operator==(const TextSearchArgument& rhs) const;

  std::string toString() const;

 private:
  friend ObjectPool;
  TextSearchArgument(const std::string& index, const std::string& query)
      : index_(index), query_(query) {}

 private:
  std::string index_;
  std::string query_;
};

class TextSearchExpression : public Expression {
 public:
  static TextSearchExpression* makeQuery(ObjectPool* pool, TextSearchArgument* arg) {
    return pool->makeAndAdd<TextSearchExpression>(pool, Kind::kESQUERY, arg);
  }

  static TextSearchExpression* make(ObjectPool* pool, Kind kind, TextSearchArgument* arg) {
    return pool->makeAndAdd<TextSearchExpression>(pool, kind, arg);
  }

  bool operator==(const Expression& rhs) const override;

  const Value& eval(ExpressionContext&) override {
    DLOG(FATAL) << "TextSearchExpression has to be rewritten";
    return Value::kNullBadData;
  }

  void accept(ExprVisitor*) override {
    DLOG(FATAL) << "TextSearchExpression has to be rewritten";
  }

  std::string toString() const override;

  Expression* clone() const override {
    auto arg = TextSearchArgument::make(pool_, arg_->index(), arg_->query());
    return TextSearchExpression::make(pool_, kind_, arg);
  }

  const TextSearchArgument* arg() const {
    return arg_;
  }

  TextSearchArgument* arg() {
    return arg_;
  }

  void setArgs(TextSearchArgument* arg) {
    arg_ = arg;
  }

 private:
  friend ObjectPool;
  TextSearchExpression(ObjectPool* pool, Kind kind, TextSearchArgument* arg)
      : Expression(pool, kind) {
    arg_ = arg;
  }

  void writeTo(Encoder&) const override {
    LOG(FATAL) << "TextSearchExpression has to be rewritten";
  }

  void resetFrom(Decoder&) override {
    LOG(FATAL) << "TextSearchExpression has to be reset";
  }

 private:
  TextSearchArgument* arg_{nullptr};
};

}  // namespace nebula
#endif  // COMMON_EXPRESSION_TEXTSEARCHEXPRESSION_H_
