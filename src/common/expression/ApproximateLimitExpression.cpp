/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/ApproximateLimitExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& ApproximateLimitExpression::eval(ExpressionContext& ctx) {
  return limitExpr_->eval(ctx);
}

bool ApproximateLimitExpression::operator==(const Expression& rhs) const {
  if (kind() != rhs.kind()) {
    return false;
  }
  const auto& r = static_cast<const ApproximateLimitExpression&>(rhs);
  return limitExpr_ == r.limitExpr_ && metricType_ == r.metricType_ &&
         annIndexType_ == r.annIndexType_ && param_ == r.param_;
}

std::string ApproximateLimitExpression::toString() const {
  std::stringstream out;
  out << " APPROXIMATE LIMIT " << limitExpr_->toString();
  out << " OPTIONS {ANNINDEX_TYPE:\"" << apache::thrift::util::enumNameSafe(annIndexType_)
      << "\", METRIC_TYPE:\"" << apache::thrift::util::enumNameSafe(metricType_)
      << "\", PARAM:" << param_ << "}";
  return out.str();
}

void ApproximateLimitExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

Expression* ApproximateLimitExpression::clone() const {
  auto limitClone = limitExpr_ ? limitExpr_->clone() : nullptr;
  auto* queryParam =
      pool_->makeAndAdd<AnnIndexQueryParamItem>(metricType_, annIndexType_, Value(param_));
  return new ApproximateLimitExpression(pool_, limitClone, queryParam);
}

void ApproximateLimitExpression::writeTo(Encoder& encoder) const {
  // Encode the inner limit expression
  UNUSED(encoder);
}

void ApproximateLimitExpression::resetFrom(Decoder& decoder) {
  // Decode the inner limit expression
  UNUSED(decoder);
}

}  // namespace nebula
