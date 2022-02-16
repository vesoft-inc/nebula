/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartRe...
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartRe...

#include <memory>  // for allocator

#include "common/datatypes/Value.h"                        // for Value, Val...
#include "common/expression/ConstantExpression.h"          // for ConstantEx...
#include "common/expression/Expression.h"                  // for Expression
#include "common/expression/UnaryExpression.h"             // for UnaryExpre...
#include "common/expression/VariableExpression.h"          // for VersionedV...
#include "common/expression/test/ExpressionContextMock.h"  // for Expression...
#include "common/expression/test/TestBase.h"               // for pool, gExp...

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
