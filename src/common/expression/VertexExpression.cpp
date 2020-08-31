/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/VertexExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& VertexExpression::eval(ExpressionContext &ctx) {
    result_ = ctx.getVertex();
    return result_;
}

void VertexExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

}   // namespace nebula
