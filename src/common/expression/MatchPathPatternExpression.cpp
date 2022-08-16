/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/MatchPathPatternExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& MatchPathPatternExpression::eval(ExpressionContext& ctx) {
  result_ = DCHECK_NOTNULL(genList_)->eval(ctx);
  return result_;
}

bool MatchPathPatternExpression::operator==(const Expression& rhs) const {
  if (kind() != rhs.kind()) {
    return false;
  }

  if (matchPath_ != matchPath_) {
    return false;
  }

  // The genList_ field is used for evaluation internally, so it don't identify the expression.
  // We don't compare it here.
  // Ditto for result_ field.

  return true;
}

std::string MatchPathPatternExpression::toString() const {
  return matchPath_->toString();
}

void MatchPathPatternExpression::accept(ExprVisitor* visitor) {
  visitor->visit(this);
}

Expression* MatchPathPatternExpression::clone() const {
  auto expr =
      MatchPathPatternExpression::make(pool_, std::make_unique<MatchPath>(matchPath_->clone()));
  if (genList_ != nullptr) {
    expr->setGenList(genList_->clone());
  }
  return expr;
}

}  // namespace nebula
