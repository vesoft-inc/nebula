/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_SFIDEXPRESSION_H_
#define COMMON_EXPRESSION_SFIDEXPRESSION_H_

#include "common/expression/Expression.h"
#include "graph/context/QueryContext.h"

namespace nebula {

class SFIDExpression : public Expression {
  friend class Expression;

 public:
  static void init(graph::QueryContext* qctx) {
    const string& ip = qctx->getMetaClient()->getLocalIp();
    auto result = qctx->getMetaClient()->getWorkerId(ip).get();
    if (!result.ok()) {
      LOG(FATAL) << "Failed to get worker id from meta server";
    }
    workerId_ = result.value().get_workerid();
  }

  static SFIDExpression* make(ObjectPool* pool) { return pool->add(new SFIDExpression(pool)); }

  bool operator==(const Expression& rhs) const override;

  const Value& eval(ExpressionContext& ctx) override;

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override { return SFIDExpression::make(pool_); }

 private:
  explicit SFIDExpression(ObjectPool* pool) : Expression(pool, Kind::kSFID) {}

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

 private:
  static int64_t workerId_;
  Value result_;
};

}  // namespace nebula
#endif  // EXPRESSION_SFIDEXPRESSION_H_
