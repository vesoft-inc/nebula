/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/VertexExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& VertexExpression::eval(ExpressionContext& ctx) {
  result_ = ctx.getVertex(name_);
  return result_;
}

bool VertexExpression::operator==(const Expression& rhs) const {
  if (kind_ != rhs.kind()) {
    return false;
  }
  const auto& expr = static_cast<const VertexExpression&>(rhs);
  return name_ == expr.name();
}

void VertexExpression::writeTo(Encoder& encoder) const {
  // kind_
  encoder << kind_;

  // name_
  encoder << name_;
}

void VertexExpression::resetFrom(Decoder& decoder) {
  // Read name_
  name_ = decoder.readStr();
}

void VertexExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

}  // namespace nebula
