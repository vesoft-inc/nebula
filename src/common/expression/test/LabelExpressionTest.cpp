/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

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
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
