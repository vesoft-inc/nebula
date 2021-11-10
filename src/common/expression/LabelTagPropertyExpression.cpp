/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/expression/LabelTagPropertyExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& LabelTagPropertyExpression::eval(ExpressionContext& ctx) {
  const auto& var = label_->eval(ctx);
  if (var.type() != Value::Type::VERTEX) {
    return Value::kNullBadType;
  }
  for (const auto& tag : var.getVertex().tags) {
    if (tag.name == tag_) {
      auto iter = tag.props.find(prop_);
      if (iter != tag.props.end()) {
        return iter->second;
      }
    }
  }
  return Value::kNullUnknownProp;
}

std::string LabelTagPropertyExpression::toString() const {
  std::string labelStr = label_->toString();
  return labelStr.erase(0, 1) + "." + tag_ + "." + prop_;
}

bool LabelTagPropertyExpression::operator==(const Expression& rhs) const {
  if (kind_ != rhs.kind()) {
    return false;
  }
  const auto& expr = static_cast<const LabelTagPropertyExpression&>(rhs);
  return *label_ == *expr.label_ && tag_ == expr.tag_ && prop_ == expr.prop_;
}

void LabelTagPropertyExpression::writeTo(Encoder& encoder) const {
  encoder << kind_;
  encoder << *label_;
  encoder << tag_;
  encoder << prop_;
}

void LabelTagPropertyExpression::resetFrom(Decoder& decoder) {
  label_ = decoder.readExpression(pool_);
  tag_ = decoder.readStr();
  prop_ = decoder.readStr();
}

void LabelTagPropertyExpression::accept(ExprVisitor* visitor) { visitor->visit(this); }

}  // namespace nebula
