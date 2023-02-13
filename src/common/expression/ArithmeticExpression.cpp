/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/ArithmeticExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& ArithmeticExpression::eval(ExpressionContext& ctx) {
  auto& lhs = lhs_->eval(ctx);
  auto& rhs = rhs_->eval(ctx);

  switch (kind_) {
    case Kind::kAdd:
      result_ = lhs + rhs;
      break;
    case Kind::kMinus:
      result_ = lhs - rhs;
      break;
    case Kind::kMultiply:
      result_ = lhs * rhs;
      break;
    case Kind::kDivision:
      result_ = lhs / rhs;
      break;
    case Kind::kMod:
      result_ = lhs % rhs;
      break;
    default:
      DLOG(FATAL) << "Unknown type: " << kind_;
      result_ = Value::kNullBadType;
  }
  return result_;
}

std::string ArithmeticExpression::toString() const {
  std::string op;
  switch (kind_) {
    case Kind::kAdd:
      op = "+";
      break;
    case Kind::kMinus:
      op = "-";
      break;
    case Kind::kMultiply:
      op = "*";
      break;
    case Kind::kDivision:
      op = "/";
      break;
    case Kind::kMod:
      op = "%";
      break;
    default:
      DLOG(FATAL) << "Illegal kind for arithmetic expression: " << static_cast<int>(kind());
      op = " Invalid arithmetic expression ";
  }
  std::stringstream out;
  out << "(" << (lhs_ ? lhs_->toString() : "") << op << (rhs_ ? rhs_->toString() : "") << ")";
  return out.str();
}

void ArithmeticExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

}  // namespace nebula
