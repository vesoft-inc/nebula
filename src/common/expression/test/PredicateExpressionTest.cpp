/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class PredicateExpressionTest : public ExpressionTest {};

TEST_F(PredicateExpressionTest, PredicateEvaluate) {
    {
        // all(n IN [0, 1, 2, 4, 5) WHERE n >= 2)
        auto *listItems = ExpressionList::make(&pool);
        (*listItems)
            .add(ConstantExpression::make(&pool, 0))
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 4))
            .add(ConstantExpression::make(&pool, 5));
        auto expr = PredicateExpression::make(
            &pool,
            "all",
            "n",
            ListExpression::make(&pool, listItems),
            RelationalExpression::makeGE(
                &pool, VariableExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)));

        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value.getBool());
    }
    {
        // any(n IN nodes(p) WHERE n.age >= 19)
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
        auto expr = PredicateExpression::make(
            &pool,
            "any",
            "n",
            FunctionCallExpression::make(&pool, "nodes", argList),
            RelationalExpression::makeGE(
                &pool,
                AttributeExpression::make(&pool,
                                          VariableExpression::make(&pool, "n"),
                                          ConstantExpression::make(&pool, "age")),
                ConstantExpression::make(&pool, 19)));

        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value.getBool());
    }
    {
        // single(n IN [0, 1, 2, 4, 5) WHERE n == 2)
        auto *listItems = ExpressionList::make(&pool);
        (*listItems)
            .add(ConstantExpression::make(&pool, 0))
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 4))
            .add(ConstantExpression::make(&pool, 5));
        auto expr = PredicateExpression::make(
            &pool,
            "single",
            "n",
            ListExpression::make(&pool, listItems),
            RelationalExpression::makeEQ(
                &pool, VariableExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)));

        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value.getBool());
    }
    {
        // none(n IN nodes(p) WHERE n.age >= 19)
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
        auto expr = PredicateExpression::make(
            &pool,
            "none",
            "n",
            FunctionCallExpression::make(&pool, "nodes", argList),
            RelationalExpression::makeGE(
                &pool,
                AttributeExpression::make(&pool,
                                          VariableExpression::make(&pool, "n"),
                                          ConstantExpression::make(&pool, "age")),
                ConstantExpression::make(&pool, 19)));

        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value.getBool());
    }
    {
        // single(n IN null WHERE n > 1)
        auto expr = PredicateExpression::make(
            &pool,
            "all",
            "n",
            ConstantExpression::make(&pool, Value(NullType::__NULL__)),
            RelationalExpression::makeEQ(
                &pool, VariableExpression::make(&pool, "n"), ConstantExpression::make(&pool, 1)));

        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(Value::kNullValue, value.getNull());
    }
}

TEST_F(PredicateExpressionTest, PredicateExprToString) {
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        argList->addArgument(ConstantExpression::make(&pool, 1));
        argList->addArgument(ConstantExpression::make(&pool, 5));
        auto expr = PredicateExpression::make(
            &pool,
            "all",
            "n",
            FunctionCallExpression::make(&pool, "range", argList),
            RelationalExpression::makeGE(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2)));
        ASSERT_EQ("all(n IN range(1,5) WHERE (n>=2))", expr->toString());
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
