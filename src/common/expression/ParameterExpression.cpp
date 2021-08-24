/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/ParameterExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& ParameterExpression::eval(ExpressionContext& ectx) { return ectx.getVar(name_); }

std::string ParameterExpression::toString() const { return '$' + name_; }

bool ParameterExpression::operator==(const Expression& rhs) const {
  if (kind_ != rhs.kind()) {
    return false;
  }
  const auto& expr = static_cast<const ParameterExpression&>(rhs);
  return name_ == expr.name();
}

void ParameterExpression::writeTo(Encoder& encoder) const {
  encoder << kind_;
  encoder << name_;
}

void ParameterExpression::resetFrom(Decoder& decoder) { name_ = decoder.readStr(); }

void ParameterExpression::accept(ExprVisitor* visitor) { visitor->visit(this); }

}  // namespace nebula
