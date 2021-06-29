/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class UnaryExpressionTest : public ExpressionTest {};

TEST_F(UnaryExpressionTest, IsNull) {
    {
        auto expr =
            *UnaryExpression::makeIsNull(&pool, ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = UnaryExpression::makeIsNull(&pool, ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = UnaryExpression::makeIsNull(&pool, ConstantExpression::make(&pool, 1.1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = UnaryExpression::makeIsNull(&pool, ConstantExpression::make(&pool, true));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr =
            *UnaryExpression::makeIsNull(&pool, ConstantExpression::make(&pool, Value::kEmpty));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
}

TEST_F(UnaryExpressionTest, IsNotNull) {
    {
        auto expr = UnaryExpression::makeIsNotNull(
            &pool, ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = UnaryExpression::makeIsNotNull(&pool, ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = UnaryExpression::makeIsNotNull(&pool, ConstantExpression::make(&pool, 1.1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = UnaryExpression::makeIsNotNull(&pool, ConstantExpression::make(&pool, true));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr =
            *UnaryExpression::makeIsNotNull(&pool, ConstantExpression::make(&pool, Value::kEmpty));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
}

TEST_F(UnaryExpressionTest, IsEmpty) {
    {
        auto expr = UnaryExpression::makeIsEmpty(
            &pool, ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = UnaryExpression::makeIsEmpty(&pool, ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = UnaryExpression::makeIsEmpty(&pool, ConstantExpression::make(&pool, 1.1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr = UnaryExpression::makeIsEmpty(&pool, ConstantExpression::make(&pool, true));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto expr =
            *UnaryExpression::makeIsEmpty(&pool, ConstantExpression::make(&pool, Value::kEmpty));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
}

TEST_F(UnaryExpressionTest, IsNotEmpty) {
    {
        auto expr = UnaryExpression::makeIsNotEmpty(
            &pool, ConstantExpression::make(&pool, Value::kNullValue));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = UnaryExpression::makeIsNotEmpty(&pool, ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = UnaryExpression::makeIsNotEmpty(&pool, ConstantExpression::make(&pool, 1.1));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr = UnaryExpression::makeIsNotEmpty(&pool, ConstantExpression::make(&pool, true));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto expr =
            *UnaryExpression::makeIsNotEmpty(&pool, ConstantExpression::make(&pool, Value::kEmpty));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
}

TEST_F(UnaryExpressionTest, UnaryINCR) {
    {
        // ++var_int
        auto expr = UnaryExpression::makeIncr(&pool, VariableExpression::make(&pool, "var_int"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 2);
    }
    {
        // ++versioned_var{0}
        auto expr = UnaryExpression::makeIncr(
            &pool,
            VersionedVariableExpression::make(
                &pool, "versioned_var", ConstantExpression::make(&pool, 0)));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 2);
    }
}

TEST_F(UnaryExpressionTest, UnaryDECR) {
    {
        // --var_int
        auto expr = UnaryExpression::makeDecr(&pool, VariableExpression::make(&pool, "var_int"));
        auto eval = Expression::eval(expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 0);
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
