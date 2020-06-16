/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <gtest/gtest.h>
#include "common/datatypes/List.h"
#include "common/expression/test/ExpressionContextMock.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"

nebula::ExpressionContextMock gExpCtxt;

namespace nebula {

class ExpressionTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ExpressionTest, Constant) {
    {
        ConstantExpression integer(1);
        auto eval = Expression::eval(&integer, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        ConstantExpression doubl(1.0);
        auto eval = Expression::eval(&doubl, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 1.0);
    }
    {
        ConstantExpression boolean(true);
        auto eval = Expression::eval(&boolean, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        ConstantExpression boolean(false);
        auto eval = Expression::eval(&boolean, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        ConstantExpression str("abcd");
        auto eval = Expression::eval(&str, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "abcd");
    }
    // TODO(cpw): EMPTY, NULL, DATE, DATETIME, VERTEX, EDGE, PATH, LIST, MAP, SET, DATASET
}

TEST_F(ExpressionTest, GetProp) {
    {
        EdgePropertyExpression ep(new std::string("e1"), new std::string("int"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
}

TEST_F(ExpressionTest, Arithmetics) {
    {
        // 1+ 2
        ArithmeticExpression add(
                Expression::Kind::kAdd, new ConstantExpression(1), new ConstantExpression(2));
        auto eval = Expression::eval(&add, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 3);
    }
    {
        // 1 + 2 + 3
        ArithmeticExpression add(
                Expression::Kind::kAdd,
                new ArithmeticExpression(
                    Expression::Kind::kAdd,
                    new ConstantExpression(1), new ConstantExpression(2)),
                new ConstantExpression(3));
        auto eval = Expression::eval(&add, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 6);
    }
    {
        // 1 + 2 + e1.int
        ArithmeticExpression add(
                Expression::Kind::kAdd,
                new ArithmeticExpression(
                    Expression::Kind::kAdd,
                    new ConstantExpression(1), new ConstantExpression(2)),
                new EdgePropertyExpression(new std::string("e1"), new std::string("int")));
        auto eval = Expression::eval(&add, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 4);
    }
    {
        // e1.string16 + e1.string16
        ArithmeticExpression add(
                Expression::Kind::kAdd,
                new EdgePropertyExpression(new std::string("e1"), new std::string("string16")),
                new EdgePropertyExpression(new std::string("e1"), new std::string("string16")));
        auto eval = Expression::eval(&add, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, std::string(32, 'a'));
    }
}

TEST_F(ExpressionTest, RelationEQ) {
    {
        // e1.list == NULL
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new EdgePropertyExpression(new std::string("e1"), new std::string("list")),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // e1.list_of_list == NULL
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new EdgePropertyExpression(new std::string("e1"), new std::string("list_of_list")),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // e1.list == e1.list
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new EdgePropertyExpression(new std::string("e1"), new std::string("list")),
                new EdgePropertyExpression(new std::string("e1"), new std::string("list")));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // e1.list_of_list == e1.list_of_list
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new EdgePropertyExpression(new std::string("e1"), new std::string("list_of_list")),
                new EdgePropertyExpression(new std::string("e1"), new std::string("list_of_list")));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
}

TEST_F(ExpressionTest, RelationIN) {
    {
        // 1 IN [1, 2, 3]
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(1),
                new ConstantExpression(Value(List(std::vector<Value>{1, 2, 3}))));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // 5 IN [1, 2, 3]
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(5),
                new ConstantExpression(Value(List(std::vector<Value>{1, 2, 3}))));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
}

TEST_F(ExpressionTest, UnaryINCR) {
    {
        // ++var_int
        UnaryExpression expr(
                Expression::Kind::kUnaryIncr,
                new VariableExpression(new std::string("var_int")));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 2);
    }
    {
        // ++versioned_var{0}
        UnaryExpression expr(
                Expression::Kind::kUnaryIncr,
                new VersionedVariableExpression(
                    new std::string("versioned_var"),
                    new ConstantExpression(0)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 2);
    }
}

TEST_F(ExpressionTest, UnaryDECR) {
    {
        // --var_int
        UnaryExpression expr(
                Expression::Kind::kUnaryDecr,
                new VariableExpression(new std::string("var_int")));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 0);
    }
}

TEST_F(ExpressionTest, VersionedVar) {
    {
        // versioned_var{0}
        VersionedVariableExpression expr(
                new std::string("versioned_var"),
                new ConstantExpression(0));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // versioned_var{0}
        VersionedVariableExpression expr(
                new std::string("versioned_var"),
                new ConstantExpression(-1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 2);
    }
    {
        // versioned_var{0}
        VersionedVariableExpression expr(
                new std::string("versioned_var"),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 8);
    }
    {
        // versioned_var{-cnt}
        VersionedVariableExpression expr(
                new std::string("versioned_var"),
                new UnaryExpression(
                    Expression::Kind::kUnaryNegate,
                    new VariableExpression(new std::string("cnt"))));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 2);
    }
}
// TODO(cpw): more test cases.
}  // namespace nebula
