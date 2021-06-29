/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class ReduceExpressionTest : public ExpressionTest {};

TEST_F(ReduceExpressionTest, ReduceEvaluate) {
    {
        // reduce(totalNum = 2 * 10, n IN range(1, 5) | totalNum + n * 2)
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(ConstantExpression::make(&pool, 1));
        argList->addArgument(ConstantExpression::make(&pool, 5));
        auto expr = ReduceExpression::make(
            &pool,
            "totalNum",
            ArithmeticExpression::makeMultiply(
                &pool, ConstantExpression::make(&pool, 2), ConstantExpression::make(&pool, 10)),
            "n",
            FunctionCallExpression::make(&pool, "range", argList),
            ArithmeticExpression::makeAdd(
                &pool,
                VariableExpression::make(&pool, "totalNum"),
                ArithmeticExpression::makeMultiply(&pool,
                                                   VariableExpression::make(&pool, "n"),
                                                   ConstantExpression::make(&pool, 2))));

        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(Value::Type::INT, value.type());
        ASSERT_EQ(50, value.getInt());
    }
}

TEST_F(ReduceExpressionTest, ReduceExprToString) {
    {
        // reduce(totalNum = 2 * 10, n IN range(1, 5) | totalNum + n * 2)
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(ConstantExpression::make(&pool, 1));
        argList->addArgument(ConstantExpression::make(&pool, 5));
        auto expr = ReduceExpression::make(
            &pool,
            "totalNum",
            ArithmeticExpression::makeMultiply(
                &pool, ConstantExpression::make(&pool, 2), ConstantExpression::make(&pool, 10)),
            "n",
            FunctionCallExpression::make(&pool, "range", argList),
            ArithmeticExpression::makeAdd(
                &pool,
                LabelExpression::make(&pool, "totalNum"),
                ArithmeticExpression::makeMultiply(
                    &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2))));
        ASSERT_EQ("reduce(totalNum = (2*10), n IN range(1,5) | (totalNum+(n*2)))",
                  expr->toString());
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
