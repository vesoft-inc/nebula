/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class ConstantExpressionTest : public ExpressionTest {};

TEST_F(ExpressionTest, Constant) {
    {
        auto integer = ConstantExpression::make(&pool, 1);
        auto eval = Expression::eval(integer, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        auto doubl = ConstantExpression::make(&pool, 1.0);
        auto eval = Expression::eval(doubl, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 1.0);
    }
    {
        auto boolean = ConstantExpression::make(&pool, true);
        auto eval = Expression::eval(boolean, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto boolean = ConstantExpression::make(&pool, false);
        auto eval = Expression::eval(boolean, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto str = ConstantExpression::make(&pool, "abcd");
        auto eval = Expression::eval(str, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "abcd");
    }
    {
        Value emptyValue;
        auto empty = ConstantExpression::make(&pool, emptyValue);
        auto eval = Expression::eval(empty, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::__EMPTY__);
        EXPECT_EQ(eval, emptyValue);
    }
    {
        NullType null;
        auto nul = ConstantExpression::make(&pool, null);
        auto eval = Expression::eval(nul, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, null);
    }
    {
        auto date = ConstantExpression::make(&pool, Date(1234));
        auto eval = Expression::eval(date, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::DATE);
        EXPECT_EQ(eval, Date(1234));
    }
    {
        Time time;
        time.hour = 3;
        time.minute = 33;
        time.sec = 3;
        auto timeExpr = ConstantExpression::make(&pool, time);
        auto eval = Expression::eval(timeExpr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::TIME);
        EXPECT_EQ(eval, time);
    }
    {
        DateTime dateTime;
        dateTime.year = 1900;
        dateTime.month = 2;
        dateTime.day = 23;
        auto datetime = ConstantExpression::make(&pool, dateTime);
        auto eval = Expression::eval(datetime, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::DATETIME);
        EXPECT_EQ(eval, dateTime);
    }
    {
        List listValue(std::vector<Value>{1, 2, 3});
        auto list = ConstantExpression::make(&pool, listValue);
        auto eval = Expression::eval(list, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::LIST);
        EXPECT_EQ(eval, listValue);
    }
    {
        Map mapValue;
        auto map = ConstantExpression::make(&pool, mapValue);
        auto eval = Expression::eval(map, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::MAP);
        EXPECT_EQ(eval, mapValue);
    }
    {
        Set setValue;
        auto set = ConstantExpression::make(&pool, setValue);
        auto eval = Expression::eval(set, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::SET);
        EXPECT_EQ(eval, setValue);
    }
}

}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
