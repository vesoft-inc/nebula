/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/expression/EdgeExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value &EdgeExpression::eval(ExpressionContext &ctx) {
  result_ = ctx.getEdge();
  return result_;
}

void EdgeExpression::accept(ExprVisitor *visitor) {
  visitor->visit(this);
}

}  // namespace nebula
