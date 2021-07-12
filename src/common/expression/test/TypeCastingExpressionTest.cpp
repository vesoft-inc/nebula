/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class TypeCastingExpressionTest : public ExpressionTest {};

TEST_F(TypeCastingExpressionTest, TypeCastTest) {
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, 1));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, 1.23));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, "1.23"));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, "123"));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 123);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, ".123"));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 0);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, ".123ab"));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, "abc123"));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, "123abc"));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, true));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }

    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::FLOAT, ConstantExpression::make(&pool, 1.23));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 1.23);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::FLOAT, ConstantExpression::make(&pool, 2));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 2.0);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::FLOAT, ConstantExpression::make(&pool, "1.23"));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 1.23);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::BOOL, ConstantExpression::make(&pool, 2));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::BOOL, ConstantExpression::make(&pool, 0));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::STRING, ConstantExpression::make(&pool, true));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "true");
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::STRING, ConstantExpression::make(&pool, false));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "false");
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::STRING, ConstantExpression::make(&pool, 12345));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "12345");
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::STRING, ConstantExpression::make(&pool, 34.23));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "34.23");
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::STRING, ConstantExpression::make(&pool, .23));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "0.23");
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::STRING, ConstantExpression::make(&pool, "ABC"));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "ABC");
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::SET, ConstantExpression::make(&pool, 23));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto typeCast = TypeCastingExpression::make(
            &pool, Value::Type::INT, ConstantExpression::make(&pool, Set()));
        auto eval = Expression::eval(typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
