/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class ArithmeticExpressionTest : public ExpressionTest {};

TEST_F(ArithmeticExpressionTest, TestArithmeticExpression) {
    {
        TEST_EXPR(123, 123);
        TEST_EXPR(-123, -123);
        TEST_EXPR(12.23, 12.23);
        TEST_EXPR(143., 143.);
    }
    {
        TEST_EXPR(10 % 3, 1);
        TEST_EXPR(10 + 3, 13);
        TEST_EXPR(1 - 4, -3);
        TEST_EXPR(11 * 2, 22);
        TEST_EXPR(11 * 2.2, 24.2);
        TEST_EXPR(100.4 / 4, 25.1);
        TEST_EXPR(10.4 % 0, NullType::DIV_BY_ZERO);
        TEST_EXPR(10 % 0.0, NullType::DIV_BY_ZERO);
        TEST_EXPR(10.4 % 0.0, NullType::DIV_BY_ZERO);
        TEST_EXPR(10 / 0, NullType::DIV_BY_ZERO);
        TEST_EXPR(12 / 0.0, NullType::DIV_BY_ZERO);
        TEST_EXPR(187. / 0.0, NullType::DIV_BY_ZERO);
        TEST_EXPR(17. / 0, NullType::DIV_BY_ZERO);
    }
    {
        TEST_EXPR(1 + 2 + 3.2, 6.2);
        TEST_EXPR(3 * 4 - 6, 6);
        TEST_EXPR(76 - 100 / 20 * 4, 56);
        TEST_EXPR(17 % 7 + 4 - 2, 5);
        TEST_EXPR(17 + 7 % 4 - 2, 18);
        TEST_EXPR(17 + 7 + 4 % 2, 24);
        TEST_EXPR(3.14 * 3 * 3 / 2, 14.13);
        TEST_EXPR(16 * 8 + 4 - 2, 130);
        TEST_EXPR(16 + 8 * 4 - 2, 46);
        TEST_EXPR(16 + 8 + 4 * 2, 32);
    }
    {
        TEST_EXPR(16 + 8 * (4 - 2), 32);
        TEST_EXPR(16 * (8 + 4) - 2, 190);
        TEST_EXPR(2 * (4 + 3) - 6, 8);
        TEST_EXPR((3 + 5) * 3 / (6 - 2), 6);
    }
    {
        // 1 + 2 + e1.int
        auto add = ArithmeticExpression::makeAdd(
            &pool,
            ArithmeticExpression::makeAdd(
                &pool, ConstantExpression::make(&pool, 1), ConstantExpression::make(&pool, 2)),
            EdgePropertyExpression::make(&pool, "e1", "int"));
        auto eval = Expression::eval(add, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 4);
    }
    {
        // e1.string16 + e1.string16
        auto add =
            ArithmeticExpression::makeAdd(&pool,
                                          EdgePropertyExpression::make(&pool, "e1", "string16"),
                                          EdgePropertyExpression::make(&pool, "e1", "string16"));
        auto eval = Expression::eval(add, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, std::string(32, 'a'));
    }
    {
        // $^.source.string16 + $$.dest.string16
        auto add = ArithmeticExpression::makeAdd(
            &pool,
            SourcePropertyExpression::make(&pool, "source", "string16"),
            DestPropertyExpression::make(&pool, "dest", "string16"));
        auto eval = Expression::eval(add, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, std::string(32, 'a'));
    }
    {
        // 10 - e1.int
        auto minus =
            ArithmeticExpression::makeMinus(&pool,
                                            ConstantExpression::make(&pool, 10),
                                            EdgePropertyExpression::make(&pool, "e1", "int"));
        auto eval = Expression::eval(minus, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 9);
    }
    {
        // 10 - $^.source.int
        auto minus =
            ArithmeticExpression::makeMinus(&pool,
                                            ConstantExpression::make(&pool, 10),
                                            SourcePropertyExpression::make(&pool, "source", "int"));
        auto eval = Expression::eval(minus, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 9);
    }
    {
        // e1.string128 - e1.string64
        auto minus =
            ArithmeticExpression::makeMinus(&pool,
                                            EdgePropertyExpression::make(&pool, "e1", "string128"),
                                            EdgePropertyExpression::make(&pool, "e1", "string64"));
        auto eval = Expression::eval(minus, gExpCtxt);
        EXPECT_NE(eval.type(), Value::Type::STRING);
        EXPECT_NE(eval, std::string(64, 'a'));
    }
    {
        // $^.source.srcProperty % $$.dest.dstProperty
        auto mod = ArithmeticExpression::makeMod(
            &pool,
            SourcePropertyExpression::make(&pool, "source", "srcProperty"),
            DestPropertyExpression::make(&pool, "dest", "dstProperty"));
        auto eval = Expression::eval(mod, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
}
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
