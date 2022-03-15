/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class VersionedVariableExpressionTest : public ExpressionTest {};

TEST_F(VersionedVariableExpressionTest, VersionedVar) {
  {
    // versioned_var{0}
    auto expr = VersionedVariableExpression::make(
        &pool, "versioned_var", ConstantExpression::make(&pool, 0));
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // versioned_var{0}
    auto expr = VersionedVariableExpression::make(
        &pool, "versioned_var", ConstantExpression::make(&pool, -1));
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 2);
  }
  {
    // versioned_var{0}
    auto expr = VersionedVariableExpression::make(
        &pool, "versioned_var", ConstantExpression::make(&pool, 1));
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 8);
  }
  {
    // versioned_var{-cnt}
    auto expr = VersionedVariableExpression::make(
        &pool,
        "versioned_var",
        UnaryExpression::makeNegate(&pool, VariableExpression::make(&pool, "cnt")));
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 2);
  }
}
}  // namespace nebula
