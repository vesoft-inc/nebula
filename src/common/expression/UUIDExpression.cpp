/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/UUIDExpression.h"

#include <folly/String.h>  // for stringPrintf

#include "common/base/Base.h"               // for UNUSED
#include "common/expression/ExprVisitor.h"  // for ExprVisitor

namespace nebula {
class ExpressionContext;

class ExpressionContext;

bool UUIDExpression::operator==(const Expression& rhs) const {
  return kind_ == rhs.kind();
}

void UUIDExpression::writeTo(Encoder& encoder) const {
  // kind_
  UNUSED(encoder);
  encoder << kind_;
}

void UUIDExpression::resetFrom(Decoder& decoder) {
  UNUSED(decoder);
}

const Value& UUIDExpression::eval(ExpressionContext& ctx) {
  UNUSED(ctx);
  return result_;
}

std::string UUIDExpression::toString() const {
  return folly::stringPrintf("uuid()");
}

void UUIDExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

}  // namespace nebula
