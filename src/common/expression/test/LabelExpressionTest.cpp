/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartRe...
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartRe...

#include <memory>  // for allocator

#include "common/datatypes/Value.h"                        // for Value
#include "common/expression/Expression.h"                  // for Expression
#include "common/expression/LabelExpression.h"             // for LabelExpre...
#include "common/expression/test/ExpressionContextMock.h"  // for Expression...
#include "common/expression/test/TestBase.h"               // for pool, Expr...

namespace nebula {

class LabelExpressionTest : public ExpressionTest {};

TEST_F(LabelExpressionTest, LabelExprToString) {
  auto expr = LabelExpression::make(&pool, "name");
  ASSERT_EQ("name", expr->toString());
}

TEST_F(LabelExpressionTest, LabelEvaluate) {
  auto expr = LabelExpression::make(&pool, "name");
  auto value = Expression::eval(expr, gExpCtxt);
  ASSERT_TRUE(value.isStr());
  ASSERT_EQ("name", value.getStr());
}
}  // namespace nebula
