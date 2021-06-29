/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class CaseExpressionTest : public ExpressionTest {};

TEST_F(CaseExpressionTest, CaseExprToString) {
    {
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 24), ConstantExpression::make(&pool, 1));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setCondition(ConstantExpression::make(&pool, 23));
        ASSERT_EQ("CASE 23 WHEN 24 THEN 1 END", expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 24), ConstantExpression::make(&pool, 1));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setCondition(ConstantExpression::make(&pool, 23));
        expr->setDefault(ConstantExpression::make(&pool, 2));
        ASSERT_EQ("CASE 23 WHEN 24 THEN 1 ELSE 2 END", expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 1));
        cases->add(ConstantExpression::make(&pool, true), ConstantExpression::make(&pool, 2));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setCondition(
            RelationalExpression::makeStartsWith(&pool,
                                                 ConstantExpression::make(&pool, "nebula"),
                                                 ConstantExpression::make(&pool, "nebu")));
        expr->setDefault(ConstantExpression::make(&pool, 3));
        ASSERT_EQ(
            "CASE (\"nebula\" STARTS WITH \"nebu\") WHEN false THEN 1 WHEN true THEN 2 ELSE 3 END",
            expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 7), ConstantExpression::make(&pool, 1));
        cases->add(ConstantExpression::make(&pool, 8), ConstantExpression::make(&pool, 2));
        cases->add(ConstantExpression::make(&pool, 8), ConstantExpression::make(&pool, "jack"));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setCondition(ArithmeticExpression::makeAdd(
            &pool, ConstantExpression::make(&pool, 3), ConstantExpression::make(&pool, 5)));
        expr->setDefault(ConstantExpression::make(&pool, false));
        ASSERT_EQ("CASE (3+5) WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 8 THEN \"jack\" ELSE false END",
                  expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 18));
        auto expr = CaseExpression::make(&pool, cases);
        ASSERT_EQ("CASE WHEN false THEN 18 END", expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 18));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setDefault(ConstantExpression::make(&pool, "ok"));
        ASSERT_EQ("CASE WHEN false THEN 18 ELSE \"ok\" END", expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(RelationalExpression::makeStartsWith(&pool,
                                                        ConstantExpression::make(&pool, "nebula"),
                                                        ConstantExpression::make(&pool, "nebu")),
                   ConstantExpression::make(&pool, "yes"));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setDefault(ConstantExpression::make(&pool, false));
        ASSERT_EQ("CASE WHEN (\"nebula\" STARTS WITH \"nebu\") THEN \"yes\" ELSE false END",
                  expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(
            RelationalExpression::makeLT(
                &pool, ConstantExpression::make(&pool, 23), ConstantExpression::make(&pool, 17)),
            ConstantExpression::make(&pool, 1));
        cases->add(
            RelationalExpression::makeEQ(
                &pool, ConstantExpression::make(&pool, 37), ConstantExpression::make(&pool, 37)),
            ConstantExpression::make(&pool, 2));
        cases->add(
            RelationalExpression::makeNE(
                &pool, ConstantExpression::make(&pool, 45), ConstantExpression::make(&pool, 99)),
            ConstantExpression::make(&pool, 3));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setDefault(ConstantExpression::make(&pool, 4));
        ASSERT_EQ("CASE WHEN (23<17) THEN 1 WHEN (37==37) THEN 2 WHEN (45!=99) THEN 3 ELSE 4 END",
                  expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(
            RelationalExpression::makeLT(
                &pool, ConstantExpression::make(&pool, 23), ConstantExpression::make(&pool, 17)),
            ConstantExpression::make(&pool, 1));
        auto expr = CaseExpression::make(&pool, cases, false);
        expr->setDefault(ConstantExpression::make(&pool, 2));
        ASSERT_EQ("((23<17) ? 1 : 2)", expr->toString());
    }
    {
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 1));
        auto expr = CaseExpression::make(&pool, cases, false);
        expr->setDefault(ConstantExpression::make(&pool, "ok"));
        ASSERT_EQ("(false ? 1 : \"ok\")", expr->toString());
    }
}

TEST_F(CaseExpressionTest, CaseEvaluate) {
    {
        // CASE 23 WHEN 24 THEN 1 END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 24), ConstantExpression::make(&pool, 1));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setCondition(ConstantExpression::make(&pool, 23));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(value, Value::kNullValue);
    }
    {
        // CASE 23 WHEN 24 THEN 1 ELSE false END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 24), ConstantExpression::make(&pool, 1));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setCondition(ConstantExpression::make(&pool, 23));
        expr->setDefault(ConstantExpression::make(&pool, false));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(value.getBool(), false);
    }
    {
        // CASE ("nebula" STARTS WITH "nebu") WHEN false THEN 1 WHEN true THEN 2 ELSE 3 END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 1));
        cases->add(ConstantExpression::make(&pool, true), ConstantExpression::make(&pool, 2));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setCondition(
            RelationalExpression::makeStartsWith(&pool,
                                                 ConstantExpression::make(&pool, "nebula"),
                                                 ConstantExpression::make(&pool, "nebu")));
        expr->setDefault(ConstantExpression::make(&pool, 3));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(2, value.getInt());
    }
    {
        // CASE (3+5) WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 8 THEN "jack" ELSE "no" END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, 7), ConstantExpression::make(&pool, 1));
        cases->add(ConstantExpression::make(&pool, 8), ConstantExpression::make(&pool, 2));
        cases->add(ConstantExpression::make(&pool, 8), ConstantExpression::make(&pool, "jack"));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setCondition(ArithmeticExpression::makeAdd(
            &pool, ConstantExpression::make(&pool, 3), ConstantExpression::make(&pool, 5)));
        expr->setDefault(ConstantExpression::make(&pool, "no"));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(2, value.getInt());
    }
    {
        // CASE WHEN false THEN 18 END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 18));
        auto expr = CaseExpression::make(&pool, cases);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(value, Value::kNullValue);
    }
    {
        // CASE WHEN false THEN 18 ELSE ok END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 18));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setDefault(ConstantExpression::make(&pool, "ok"));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("ok", value.getStr());
    }
    {
        // CASE WHEN "invalid when" THEN "no" ELSE 3 END
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, "invalid when"),
                   ConstantExpression::make(&pool, "no"));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setDefault(ConstantExpression::make(&pool, 3));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(value, Value::kNullBadType);
    }
    {
        // CASE WHEN (23<17) THEN 1 WHEN (37==37) THEN 2 WHEN (45!=99) THEN 3 ELSE 4 END
        auto *cases = CaseList::make(&pool);
        cases->add(
            RelationalExpression::makeLT(
                &pool, ConstantExpression::make(&pool, 23), ConstantExpression::make(&pool, 17)),
            ConstantExpression::make(&pool, 1));
        cases->add(
            RelationalExpression::makeEQ(
                &pool, ConstantExpression::make(&pool, 37), ConstantExpression::make(&pool, 37)),
            ConstantExpression::make(&pool, 2));
        cases->add(
            RelationalExpression::makeNE(
                &pool, ConstantExpression::make(&pool, 45), ConstantExpression::make(&pool, 99)),
            ConstantExpression::make(&pool, 3));
        auto expr = CaseExpression::make(&pool, cases);
        expr->setDefault(ConstantExpression::make(&pool, 4));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(2, value.getInt());
    }
    {
        // ((23<17) ? 1 : 2)
        auto *cases = CaseList::make(&pool);
        cases->add(
            RelationalExpression::makeLT(
                &pool, ConstantExpression::make(&pool, 23), ConstantExpression::make(&pool, 17)),
            ConstantExpression::make(&pool, 1));
        auto expr = CaseExpression::make(&pool, cases, false);
        expr->setDefault(ConstantExpression::make(&pool, 2));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(2, value.getInt());
    }
    {
        // (false ? 1 : "ok")
        auto *cases = CaseList::make(&pool);
        cases->add(ConstantExpression::make(&pool, false), ConstantExpression::make(&pool, 1));
        auto expr = CaseExpression::make(&pool, cases, false);
        expr->setDefault(ConstantExpression::make(&pool, "ok"));
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("ok", value.getStr());
    }
}

}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
