/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_VECTORQUERYEXPRESSION_H_
#define COMMON_EXPRESSION_VECTORQUERYEXPRESSION_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"

namespace nebula {

class VectorQueryArgument final {
 public:
  static VectorQueryArgument* make(ObjectPool* pool,
                                 const std::string& index,
                                 const std::string& field) {
    return pool->makeAndAdd<VectorQueryArgument>(index, field);
  }

  ~VectorQueryArgument() = default;

  std::string& index() {
    return index_;
  }

  std::string& field() {
    return field_;
  }

  bool operator==(const VectorQueryArgument& rhs) const;

  std::string toString() const;

 private:
  friend ObjectPool;
  VectorQueryArgument(const std::string& index, const std::string& field)
      : index_(index), field_(field) {}

 private:
  std::string index_;
  std::string field_;
};

class VectorQueryExpression final : public Expression {
 public:
  static VectorQueryExpression* makeQuery(ObjectPool* pool, VectorQueryArgument* arg) {
    return pool->makeAndAdd<VectorQueryExpression>(pool, Kind::kVectorQuery, arg);
  }

  static VectorQueryExpression* make(ObjectPool* pool, Kind kind, VectorQueryArgument* arg) {
    return pool->makeAndAdd<VectorQueryExpression>(pool, kind, arg);
  }

  bool operator==(const Expression& rhs) const override;

  const Value& eval(ExpressionContext&) override {
    DLOG(FATAL) << "VectorQueryExpression has to be rewritten";
    return Value::kNullBadData;
  }

  void accept(ExprVisitor*) override {
    DLOG(FATAL) << "VectorQueryExpression has to be rewritten";
  }

  std::string toString() const override;

  Expression* clone() const override {
    auto arg = VectorQueryArgument::make(pool_, arg_->index(), arg_->field());
    return VectorQueryExpression::make(pool_, kind_, arg);
  }

  const VectorQueryArgument* arg() const {
    return arg_;
  }

  VectorQueryArgument* arg() {
    return arg_;
  }

  void setArgs(VectorQueryArgument* arg) {
    arg_ = arg;
  }

 private:
  friend ObjectPool;
  VectorQueryExpression(ObjectPool* pool, Kind kind, VectorQueryArgument* arg)
      : Expression(pool, kind) {
    arg_ = arg;
  }

  void writeTo(Encoder&) const override {
    LOG(FATAL) << "VectorQueryExpression has to be rewritten";
  }

  void resetFrom(Decoder&) override {
    LOG(FATAL) << "VectorQueryExpression has to be reset";
  }

  VectorQueryArgument* arg_{nullptr};
};

}  // namespace nebula

#endif  // COMMON_EXPRESSION_VECTORQUERYEXPRESSION_H_ 