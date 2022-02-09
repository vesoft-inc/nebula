/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class ColumnExpressionTest : public ExpressionTest {};

TEST_F(ExpressionTest, ColumnExpression) {
  {
    auto expr = ColumnExpression::make(&pool, 2);
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 3);
  }
  {
    auto expr = ColumnExpression::make(&pool, 0);
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    auto expr = ColumnExpression::make(&pool, -1);
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 8);
  }
  {
    auto expr = ColumnExpression::make(&pool, -3);
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 6);
  }
  {
    auto expr = ColumnExpression::make(&pool, 8);
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval, Value::kNullBadType);
  }
  {
    auto expr = ColumnExpression::make(&pool, -8);
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval, Value::kNullBadType);
  }
  {
    auto expr = ColumnExpression::make(&pool, 10);
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval, Value::kNullBadType);
  }
  {
    auto expr = ColumnExpression::make(&pool, -10);
    auto eval = Expression::eval(expr, gExpCtxt);
    EXPECT_EQ(eval, Value::kNullBadType);
  }
}

}  // namespace nebula
