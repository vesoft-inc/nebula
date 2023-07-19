/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/LabelAttributeExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

std::string LabelAttributeExpression::toString() const {
  auto lhs = left();
  auto rhs = right();
  auto label = lhs ? (lhs->toString()) : "";
  std::string attr;
  if (rhs != nullptr) {
    DCHECK_EQ(rhs->kind(), Kind::kConstant);
    auto *constant = static_cast<const ConstantExpression *>(rhs);
    if (constant->value().isStr()) {
      attr = constant->value().getStr();
    } else {
      attr = rhs->toString();
    }
  }
  return label + "." + attr;
}

void LabelAttributeExpression::accept(ExprVisitor *visitor) {
  visitor->visit(this);
}

}  // namespace nebula
