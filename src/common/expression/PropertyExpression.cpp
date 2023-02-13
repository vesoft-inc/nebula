/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/PropertyExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

bool PropertyExpression::operator==(const Expression& rhs) const {
  if (kind_ != rhs.kind()) {
    return false;
  }

  const auto& r = static_cast<const PropertyExpression&>(rhs);
  return ref_ == r.ref_ && sym_ == r.sym_ && prop_ == r.prop_;
}

void PropertyExpression::writeTo(Encoder& encoder) const {
  // kind_
  encoder << kind_;

  // ref_
  encoder << ref_;

  // alias_
  encoder << sym_;

  // prop_
  encoder << prop_;
}

void PropertyExpression::resetFrom(Decoder& decoder) {
  // Read ref_
  ref_ = decoder.readStr();

  // Read alias_
  sym_ = decoder.readStr();

  // Read prop_
  prop_ = decoder.readStr();
}

const Value& PropertyExpression::eval(ExpressionContext& ctx) {
  // TODO maybe cypher need it.
  UNUSED(ctx);
  DLOG(FATAL) << "Unimplemented";
  return Value::kNullBadType;
}

const Value& EdgePropertyExpression::eval(ExpressionContext& ctx) {
  result_ = ctx.getEdgeProp(sym_, prop_);
  return result_;
}

void EdgePropertyExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& TagPropertyExpression::eval(ExpressionContext& ctx) {
  result_ = ctx.getTagProp(sym_, prop_);
  return result_;
}

void TagPropertyExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& InputPropertyExpression::eval(ExpressionContext& ctx) {
  if (!propIndex_.has_value()) {
    auto indexResult = ctx.getInputPropIndex(prop_);
    if (!indexResult.ok()) {
      return Value::kEmpty;
    }
    propIndex_ = indexResult.value();
  }
  return ctx.getColumn(propIndex_.value());
}

void InputPropertyExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& VariablePropertyExpression::eval(ExpressionContext& ctx) {
  if (!propIndex_.has_value()) {
    auto indexResult = ctx.getVarPropIndex(sym_, prop_);
    if (!indexResult.ok()) {
      return Value::kEmpty;
    }
    propIndex_ = indexResult.value();
  }
  return ctx.getColumn(propIndex_.value());
}

void VariablePropertyExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& SourcePropertyExpression::eval(ExpressionContext& ctx) {
  result_ = ctx.getSrcProp(sym_, prop_);
  return result_;
}

void SourcePropertyExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& DestPropertyExpression::eval(ExpressionContext& ctx) {
  return ctx.getDstProp(sym_, prop_);
}

void DestPropertyExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& EdgeSrcIdExpression::eval(ExpressionContext& ctx) {
  result_ = ctx.getEdgeProp(sym_, prop_);
  return result_;
}

void EdgeSrcIdExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& EdgeTypeExpression::eval(ExpressionContext& ctx) {
  result_ = ctx.getEdgeProp(sym_, prop_);
  return result_;
}

void EdgeTypeExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& EdgeRankExpression::eval(ExpressionContext& ctx) {
  result_ = ctx.getEdgeProp(sym_, prop_);
  return result_;
}

void EdgeRankExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

const Value& EdgeDstIdExpression::eval(ExpressionContext& ctx) {
  result_ = ctx.getEdgeProp(sym_, prop_);
  return result_;
}

void EdgeDstIdExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

std::string PropertyExpression::toString() const {
  std::string buf;
  buf.reserve(64);

  if (!ref_.empty()) {
    buf += ref_;
    buf += ".";
  }

  if (!sym_.empty()) {
    buf += sym_;
    buf += ".";
  }
  if (!prop_.empty()) {
    buf += prop_;
  }

  return buf;
}

std::string VariablePropertyExpression::toString() const {
  std::string buf;
  buf.reserve(64);

  if (!ref_.empty()) {
    buf += ref_;
  }
  if (!sym_.empty()) {
    buf += sym_;
    buf += ".";
  }
  if (!prop_.empty()) {
    buf += prop_;
  }

  return buf;
}

const Value& LabelTagPropertyExpression::eval(ExpressionContext& ctx) {
  const auto& var = label_->eval(ctx);
  if (var.type() != Value::Type::VERTEX) {
    return Value::kNullBadType;
  }
  for (const auto& tag : var.getVertex().tags) {
    if (tag.name == sym_) {
      auto iter = tag.props.find(prop_);
      if (iter != tag.props.end()) {
        return iter->second;
      }
    }
  }
  return Value::kNullValue;
}

void LabelTagPropertyExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

std::string LabelTagPropertyExpression::toString() const {
  std::string labelStr;
  if (label_ != nullptr) {
    labelStr = label_->toString();
    // Remove the leading '$' character for variable except '$-/$$/$^'
    if (labelStr.find(kInputRef) != 0 && labelStr.find(kSrcRef) != 0 &&
        labelStr.find(kDstRef) != 0) {
      labelStr.erase(0, 1);
    }
    labelStr += ".";
  }
  if (!sym_.empty()) {
    labelStr += sym_ + ".";
  }
  return labelStr + prop_;
}

bool LabelTagPropertyExpression::operator==(const Expression& rhs) const {
  if (kind_ != rhs.kind()) {
    return false;
  }
  const auto& expr = static_cast<const LabelTagPropertyExpression&>(rhs);
  return *label_ == *expr.label_ && sym_ == expr.sym_ && prop_ == expr.prop_;
}

void LabelTagPropertyExpression::writeTo(Encoder& encoder) const {
  PropertyExpression::writeTo(encoder);
  encoder << *label_;
}

void LabelTagPropertyExpression::resetFrom(Decoder& decoder) {
  PropertyExpression::resetFrom(decoder);
  label_ = decoder.readExpression(pool_);
}

}  // namespace nebula
