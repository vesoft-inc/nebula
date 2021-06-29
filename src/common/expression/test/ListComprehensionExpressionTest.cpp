/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class ListComprehensionExpressionTest : public ExpressionTest {};

TEST_F(ListComprehensionExpressionTest, ListComprehensionEvaluate) {
    {
        // [n IN [0, 1, 2, 4, 5] WHERE n >= 2 | n + 10]
        auto listItems = ExpressionList::make(&pool);
        (*listItems)
            .add(ConstantExpression::make(&pool, 0))
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 4))
            .add(ConstantExpression::make(&pool, 5));
        auto expr = ListComprehensionExpression::make(
            &pool,
            "n",
            ListExpression::make(&pool, listItems),
            RelationalExpression::makeGE(
                &pool, VariableExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)),
            ArithmeticExpression::makeAdd(
                &pool, VariableExpression::make(&pool, "n"), ConstantExpression::make(&pool, 10)));

        auto value = Expression::eval(expr, gExpCtxt);
        List expected;
        expected.reserve(3);
        expected.emplace_back(12);
        expected.emplace_back(14);
        expected.emplace_back(15);
        ASSERT_TRUE(value.isList());
        ASSERT_EQ(expected, value.getList());
    }
    {
        // [n IN nodes(p) | n.age + 5]
        auto v1 = Vertex("101", {Tag("player", {{"name", "joe"}, {"age", 18}})});
        auto v2 = Vertex("102", {Tag("player", {{"name", "amber"}, {"age", 19}})});
        auto v3 = Vertex("103", {Tag("player", {{"name", "shawdan"}, {"age", 20}})});
        Path path;
        path.src = v1;
        path.steps.emplace_back(Step(v2, 1, "like", 0, {}));
        path.steps.emplace_back(Step(v3, 1, "like", 0, {}));
        gExpCtxt.setVar("p", path);

        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(VariableExpression::make(&pool, "p"));
        auto expr = ListComprehensionExpression::make(
            &pool,
            "n",
            FunctionCallExpression::make(&pool, "nodes", argList),
            nullptr,
            ArithmeticExpression::makeAdd(
                &pool,
                AttributeExpression::make(&pool,
                                          VariableExpression::make(&pool, "n"),
                                          ConstantExpression::make(&pool, "age")),
                ConstantExpression::make(&pool, 5)));

        auto value = Expression::eval(expr, gExpCtxt);
        List expected;
        expected.reserve(3);
        expected.emplace_back(23);
        expected.emplace_back(24);
        expected.emplace_back(25);
        ASSERT_TRUE(value.isList());
        ASSERT_EQ(expected, value.getList());
    }
}

TEST_F(ListComprehensionExpressionTest, ListComprehensionExprToString) {
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(ConstantExpression::make(&pool, 1));
        argList->addArgument(ConstantExpression::make(&pool, 5));
        auto expr = ListComprehensionExpression::make(
            &pool,
            "n",
            FunctionCallExpression::make(&pool, "range", argList),
            RelationalExpression::makeGE(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)));
        ASSERT_EQ("[n IN range(1,5) WHERE (n>=2)]", expr->toString());
    }
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(LabelExpression::make(&pool, "p"));
        auto expr = ListComprehensionExpression::make(
            &pool,
            "n",
            FunctionCallExpression::make(&pool, "nodes", argList),
            nullptr,
            ArithmeticExpression::makeAdd(
                &pool,
                LabelAttributeExpression::make(&pool,
                                               LabelExpression::make(&pool, "n"),
                                               ConstantExpression::make(&pool, "age")),
                ConstantExpression::make(&pool, 10)));
        ASSERT_EQ("[n IN nodes(p) | (n.age+10)]", expr->toString());
    }
    {
        auto listItems = ExpressionList::make(&pool);
        (*listItems)
            .add(ConstantExpression::make(&pool, 0))
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2));
        auto expr = ListComprehensionExpression::make(
            &pool,
            "n",
            ListExpression::make(&pool, listItems),
            RelationalExpression::makeGE(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)),
            ArithmeticExpression::makeAdd(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 10)));
        ASSERT_EQ("[n IN [0,1,2] WHERE (n>=2) | (n+10)]", expr->toString());
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
