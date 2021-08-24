/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class ParameterExpressionTest : public ExpressionTest {};

TEST_F(ParameterExpressionTest, ParamExprToString) {
  auto expr = ParameterExpression::make(&pool, "param1");
  ASSERT_EQ("$param1", expr->toString());
}

TEST_F(ParameterExpressionTest, ParamEvaluate) {
  auto expr = ParameterExpression::make(&pool, "param1");
  auto value = Expression::eval(expr, gExpCtxt);
  ASSERT_TRUE(value.isInt());
  ASSERT_EQ(1, value.getInt());
}
}  // namespace nebula

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
