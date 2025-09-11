/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_EXPRESSION_APPROXIMATELIMITEXPRESSION_H_
#define COMMON_EXPRESSION_APPROXIMATELIMITEXPRESSION_H_

#include "common/expression/Expression.h"
#include "common/vectorIndex/VectorIndexUtils.h"
#include "parser/MaintainSentences.h"

namespace nebula {

class ApproximateLimitExpression final : public Expression {
 public:
  static ApproximateLimitExpression* make(ObjectPool* pool,
                                          Expression* limitExpr,
                                          AnnIndexQueryParamItem* queryParam) {
    return pool->makeAndAdd<ApproximateLimitExpression>(pool, limitExpr, queryParam);
  }

  const Expression* limitExpr() const {
    return limitExpr_;
  }

  Expression* limitExpr() {
    return limitExpr_;
  }

  MetricType metricType() const {
    return metricType_;
  }

  AnnIndexType annIndexType() const {
    return annIndexType_;
  }

  int64_t param() const {
    return param_;
  }

  const Value& eval(ExpressionContext& ctx) override;

  bool operator==(const Expression& rhs) const override;

  std::string toString() const override;

  void accept(ExprVisitor* visitor) override;

  Expression* clone() const override;

 private:
  friend ObjectPool;
  explicit ApproximateLimitExpression(ObjectPool* pool,
                                      Expression* limitExpr,
                                      AnnIndexQueryParamItem* queryParam)
      : Expression(pool, Kind::kApproximateLimit), limitExpr_(limitExpr) {
    annIndexType_ = queryParam->getAnnIndexType();
    metricType_ = queryParam->getMetricType();
    param_ = queryParam->getParam().getInt();
  }

  void writeTo(Encoder& encoder) const override;
  void resetFrom(Decoder& decoder) override;

 private:
  Expression* limitExpr_;
  AnnIndexType annIndexType_;
  MetricType metricType_;
  int64_t param_;
};

}  // namespace nebula

#endif  // COMMON_EXPRESSION_APPROXIMATELIMITEXPRESSION_H_
