/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/expression/AggregateExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

bool AggregateExpression::operator==(const Expression& rhs) const {
  if (kind_ != rhs.kind()) {
    return false;
  }

  const auto& r = static_cast<const AggregateExpression&>(rhs);
  return name_ == r.name_ && *arg_ == *(r.arg_);
}

void AggregateExpression::writeTo(Encoder& encoder) const {
  encoder << kind_;
  encoder << name_;
  encoder << Value(distinct_);
  if (arg_) {
    encoder << *arg_;
  }
}

void AggregateExpression::resetFrom(Decoder& decoder) {
  name_ = decoder.readStr();
  distinct_ = decoder.readValue().getBool();
  arg_ = decoder.readExpression(pool_);
  auto aggFuncResult = AggFunctionManager::get(name_);
  if (aggFuncResult.ok()) {
    aggFunc_ = std::move(aggFuncResult).value();
  }
}

const Value& AggregateExpression::eval(ExpressionContext& ctx) {
  DCHECK(!!aggData_);
  auto val = arg_->eval(ctx);
  auto uniques = aggData_->uniques();
  if (distinct_) {
    if (uniques->contains(val)) {
      return aggData_->result();
    }
    uniques->values.emplace(val);
  }

  DCHECK(aggFunc_);
  aggFunc_(aggData_, val);

  return aggData_->result();
}

void AggregateExpression::apply(AggData* aggData, const Value& val) {
  AggFunctionManager::get(name_).value()(aggData, val);
}

std::string AggregateExpression::toString() const {
  std::stringstream out;
  DCHECK(!!arg_);
  auto argStr = arg_->toString();
  if (arg_->kind() == Expression::Kind::kConstant &&
      static_cast<ConstantExpression*>(arg_)->value().isStr()) {
    argStr = static_cast<ConstantExpression*>(arg_)->value().getStr();
  }
  out << name_ << "(" << (distinct_ ? "distinct " : "") << argStr << ")";
  return out.str();
}

void AggregateExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}
}  // namespace nebula
