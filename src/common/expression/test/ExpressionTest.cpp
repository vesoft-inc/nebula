/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Vertex.h"
#include "common/expression/test/ExpressionContextMock.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/LabelExpression.h"

nebula::ExpressionContextMock gExpCtxt;

namespace nebula {

static void InsertSpace(std::string &str) {
    for (unsigned int i = 0; i < str.size(); i++) {
        if (str[i] == '(') {
            str.insert(i + 1, 1, ' ');
        } else if (str[i] == ')') {
            str.insert(i, 1, ' ');
            i += 1;
        } else {
            continue;
        }
    }
}

static std::vector<std::string> InfixToSuffix(const std::vector<std::string> &expr) {
    std::vector<std::string> values;
    std::stack<std::string> operators;
    std::unordered_map<std::string, int8_t> priority = {{"||", 1},
                                                        {"&&", 2},
                                                        {"^", 3},
                                                        {"==", 4},
                                                        {"!=", 4},
                                                        {">=", 5},
                                                        {"<=", 5},
                                                        {">", 5},
                                                        {"<", 5},
                                                        {"+", 6},
                                                        {"-", 6},
                                                        {"*", 7},
                                                        {"/", 7},
                                                        {"%", 7},
                                                        {"!", 8}};

    for (const auto &str : expr) {
        if (priority.find(str) != priority.end() || str == "(") {
            if (operators.empty() || str == "(") {
                operators.push(str);
            } else {
                if (operators.top() == "(" || priority[str] > priority[operators.top()]) {
                    operators.push(str);
                } else {
                    while (!operators.empty() && priority[str] <= priority[operators.top()]) {
                        values.push_back(operators.top());
                        operators.pop();
                    }
                    operators.push(str);
                }
            }
        } else if (str == ")") {
            while (!operators.empty() && operators.top() != "(") {
                values.push_back(operators.top());
                operators.pop();
            }
            operators.pop();
        } else {
            values.push_back(str);
        }
    }
    while (!operators.empty()) {
        values.push_back(operators.top());
        operators.pop();
    }
    return values;
}

class ExpressionTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

private:
    static std::unordered_map<std::string, Value> boolen_;
    static std::unordered_map<std::string, Expression::Kind> op_;

protected:
    Expression *ExpressionCalu(const std::vector<std::string> &expr) {
        std::vector<std::string> relationOp = {">", ">=", "<", "<=", "==", "!="};
        std::vector<std::string> logicalOp = {"&&", "||", "^"};
        std::vector<std::string> arithmeticOp = {"+", "-", "*", "/", "%"};

        std::vector<std::string> symbol = InfixToSuffix(expr);
        if (symbol.size() == 1) {
            // TEST_EXPR(true, true)
            if (boolen_.find(symbol.front()) != boolen_.end()) {
                return new ConstantExpression(boolen_[symbol.front()]);
            } else if (symbol.front().find('.') != std::string::npos) {
                // TEST_EXPR(123.0, 123.0)
                return new ConstantExpression(::atof(symbol.front().c_str()));
            }
            // TEST_EXPR(123, 123)
            return new ConstantExpression(::atoi(symbol.front().c_str()));
        }

        // calu suffix expression
        std::stack<Expression *> value;
        for (const auto &str : symbol) {
            if (op_.find(str) == op_.end()) {
                Expression *ep = nullptr;
                if (boolen_.find(str) != boolen_.end()) {
                    ep = new ConstantExpression(boolen_[str.c_str()]);
                } else if (str.find('.') != std::string::npos) {
                    ep = new ConstantExpression(::atof(str.c_str()));
                } else {
                    ep = new ConstantExpression(::atoi(str.c_str()));
                }
                value.push(ep);
            } else {
                Expression *result = nullptr;
                Expression *rhs = value.top();
                value.pop();
                Expression *lhs = value.top();
                value.pop();
                if (std::find(arithmeticOp.begin(), arithmeticOp.end(), str) !=
                    arithmeticOp.end()) {
                    result = new ArithmeticExpression(op_[str], lhs, rhs);
                } else if (std::find(relationOp.begin(), relationOp.end(), str) !=
                           relationOp.end()) {
                    result = new RelationalExpression(op_[str], lhs, rhs);
                } else if (std::find(logicalOp.begin(), logicalOp.end(), str) != logicalOp.end()) {
                    result = new LogicalExpression(op_[str], lhs, rhs);
                } else {
                    return new ConstantExpression(NullType::UNKNOWN_PROP);
                }
                value.push(result);
            }
        }
        return value.top();
    }

    void testExpr(const std::string &exprSymbol, Value expected) {
        std::string expr(exprSymbol);
        InsertSpace(expr);
        std::vector<std::string> splitString;
        boost::split(splitString, expr, boost::is_any_of(" \t"));
        Expression *ep = ExpressionCalu(splitString);
        auto eval = Expression::eval(ep, gExpCtxt);
        EXPECT_EQ(eval.type(), expected.type());
        EXPECT_EQ(eval, expected);
        delete ep;
    }

    void testToString(const std::string &exprSymbol, const char *expected) {
        std::string expr(exprSymbol);
        InsertSpace(expr);
        std::vector<std::string> splitString;
        boost::split(splitString, expr, boost::is_any_of(" \t"));
        Expression *ep = ExpressionCalu(splitString);
        EXPECT_EQ(ep->toString(), expected);
        delete ep;
    }

    void testFunction(const char *name, const std::vector<Value> &args, const Value &expected) {
        ArgumentList *argList = new ArgumentList();
        for (const auto &i : args) {
            argList->addArgument(std::make_unique<ConstantExpression>(std::move(i)));
        }
        FunctionCallExpression functionCall(new std::string(name), argList);
        auto eval = Expression::eval(&functionCall, gExpCtxt);
        // EXPECT_EQ(eval.type(), expected.type());
        EXPECT_EQ(eval, expected);
    }
};

std::unordered_map<std::string, Expression::Kind> ExpressionTest::op_ = {
    {"+", Expression::Kind::kAdd},
    {"-", Expression::Kind::kMinus},
    {"*", Expression::Kind::kMultiply},
    {"/", Expression::Kind::kDivision},
    {"%", Expression::Kind::kMod},
    {"||", Expression::Kind::kLogicalOr},
    {"&&", Expression::Kind::kLogicalAnd},
    {"^", Expression::Kind::kLogicalXor},
    {">", Expression::Kind::kRelGT},
    {"<", Expression::Kind::kRelLT},
    {">=", Expression::Kind::kRelGE},
    {"<=", Expression::Kind::kRelLE},
    {"==", Expression::Kind::kRelEQ},
    {"!=", Expression::Kind::kRelNE},
    {"!", Expression::Kind::kUnaryNot}};

std::unordered_map<std::string, Value> ExpressionTest::boolen_ = {{"true", Value(true)},
                                                                  {"false", Value(false)}};

static std::unordered_map<std::string, std::vector<Value>> args_ = {
    {"null", {}},
    {"int", {4}},
    {"float", {1.1}},
    {"neg_int", {-1}},
    {"neg_float", {-1.1}},
    {"rand", {1, 10}},
    {"one", {-1.2}},
    {"two", {2, 4}},
    {"pow", {2, 3}},
    {"string", {"AbcDeFG"}},
    {"trim", {" abc  "}},
    {"substr", {"abcdefghi", 2, 4}},
    {"side", {"abcdefghijklmnopq", 5}},
    {"neg_side", {"abcdefghijklmnopq", -2}},
    {"pad", {"abcdefghijkl", 16, "123"}},
    {"udf_is_in", {4, 1, 2, 8, 4, 3, 1, 0}}};

// expr -- the expression can evaluate by nGQL parser may not evaluated by c++
// expected -- the expected value of expression must evaluated by c++
#define TEST_EXPR(expr, expected)                                                                  \
    do {                                                                                           \
        testExpr(#expr, expected);                                                                 \
    } while (0);

#define TEST_FUNCTION(expr, args, expected)                                                        \
    do {                                                                                           \
        testFunction(#expr, args, expected);                                                       \
    } while (0);

#define TEST_TOSTRING(expr, expected)                                                              \
    do {                                                                                           \
        testToString(#expr, expected);                                                             \
    } while (0);

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
    {
        Value emptyValue;
        ConstantExpression empty(emptyValue);
        auto eval = Expression::eval(&empty, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::__EMPTY__);
        EXPECT_EQ(eval, emptyValue);
    }
    {
        NullType null;
        ConstantExpression nul(null);
        auto eval = Expression::eval(&nul, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, null);
    }
    {
        ConstantExpression date(Date(1234));
        auto eval = Expression::eval(&date, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::DATE);
        EXPECT_EQ(eval, Date(1234));
    }
    {
        DateTime dateTime;
        dateTime.year = 1900;
        dateTime.month = 2;
        dateTime.day = 23;
        ConstantExpression datetime(dateTime);
        auto eval = Expression::eval(&datetime, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::DATETIME);
        EXPECT_EQ(eval, dateTime);
    }
    {
        List listValue(std::vector<Value>{1, 2, 3});
        ConstantExpression list(listValue);
        auto eval = Expression::eval(&list, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::LIST);
        EXPECT_EQ(eval, listValue);
    }
    {
        Map mapValue;
        ConstantExpression map(mapValue);
        auto eval = Expression::eval(&map, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::MAP);
        EXPECT_EQ(eval, mapValue);
    }
    {
        Set setValue;
        ConstantExpression set(setValue);
        auto eval = Expression::eval(&set, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::SET);
        EXPECT_EQ(eval, setValue);
    }
}

TEST_F(ExpressionTest, GetProp) {
    {
        // e1.int
        EdgePropertyExpression ep(new std::string("e1"), new std::string("int"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // t1.int
        TagPropertyExpression ep(new std::string("t1"), new std::string("int"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // $-.int
        InputPropertyExpression ep(new std::string("int"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // $^.source.int
        SourcePropertyExpression ep(new std::string("source"), new std::string("int"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // $$.dest.int
        DestPropertyExpression ep(new std::string("dest"), new std::string("int"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // $var.float
        VariablePropertyExpression ep(new std::string("var"), new std::string("float"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 1.1);
    }
}

TEST_F(ExpressionTest, EdgeTest) {
    {
        // EdgeName._src
        EdgeSrcIdExpression ep(new std::string("edge1"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // EdgeName._type
        EdgeTypeExpression ep(new std::string("edge1"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // EdgeName._rank
        EdgeRankExpression ep(new std::string("edge1"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        // EdgeName._dst
        EdgeDstIdExpression ep(new std::string("edge1"));
        auto eval = Expression::eval(&ep, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
}

TEST_F(ExpressionTest, LogicalCalculation) {
    {
        TEST_EXPR(true, true);
        TEST_EXPR(false, false);
    }
    {
        TEST_EXPR(true ^ true, false);
        TEST_EXPR(true ^ false, true);
        TEST_EXPR(false ^ false, false);
        TEST_EXPR(false ^ true, true);
    }
    {
        TEST_EXPR(true && true && true, true);
        TEST_EXPR(true && true || false, true);
        TEST_EXPR(true && true && false, false);
        TEST_EXPR(true || false && true || false, true);
        TEST_EXPR(true ^ true ^ false, false);
    }
    {
        // AND
        TEST_EXPR(true && true, true);
        TEST_EXPR(true && false, false);
        TEST_EXPR(false && true, false);
        TEST_EXPR(false && false, false);

        // AND AND  ===  (AND) AND
        TEST_EXPR(true && true && true, true);
        TEST_EXPR(true && true && false, false);
        TEST_EXPR(true && false && true, false);
        TEST_EXPR(true && false && false, false);
        TEST_EXPR(false && true && true, false);
        TEST_EXPR(false && true && false, false);
        TEST_EXPR(false && false && true, false);
        TEST_EXPR(false && false && false, false);

        // OR
        TEST_EXPR(true || true, true);
        TEST_EXPR(true || false, true);
        TEST_EXPR(false || true, true);
        TEST_EXPR(false || false, false);

        // OR OR  ===  (OR) OR
        TEST_EXPR(true || true || true, true);
        TEST_EXPR(true || true || false, true);
        TEST_EXPR(true || false || true, true);
        TEST_EXPR(true || false || false, true);
        TEST_EXPR(false || true || true, true);
        TEST_EXPR(false || true || false, true);
        TEST_EXPR(false || false || true, true);
        TEST_EXPR(false || false || false, false);

        // AND OR  ===  (AND) OR
        TEST_EXPR(true && true || true, true);
        TEST_EXPR(true && true || false, true);
        TEST_EXPR(true && false || true, true);
        TEST_EXPR(true && false || false, false);
        TEST_EXPR(false && true || true, true);
        TEST_EXPR(false && true || false, false);
        TEST_EXPR(false && false || true, true);
        TEST_EXPR(false && false || false, false);

        // OR AND  === OR (AND)
        TEST_EXPR(true || true && true, true);
        TEST_EXPR(true || true && false, true);
        TEST_EXPR(true || false && true, true);
        TEST_EXPR(true || false && false, true);
        TEST_EXPR(false || true && true, true);
        TEST_EXPR(false || true && false, false);
        TEST_EXPR(false || false && true, false);
        TEST_EXPR(false || false && false, false);
    }
    {
        TEST_EXPR(2 > 1 && 3 > 2, true);
        TEST_EXPR(2 <= 1 && 3 > 2, false);
        TEST_EXPR(2 > 1 && 3 < 2, false);
        TEST_EXPR(2 < 1 && 3 < 2, false);
    }
}

TEST_F(ExpressionTest, LiteralConstantsRelational) {
    {
        TEST_EXPR(true == 1.0, false);
        TEST_EXPR(true == 2.0, false);
        TEST_EXPR(true != 1.0, true);
        TEST_EXPR(true != 2.0, true);
        TEST_EXPR(true > 1.0, false);
        TEST_EXPR(true >= 1.0, false);
        TEST_EXPR(true < 1.0, true);
        TEST_EXPR(true <= 1.0, true);
        TEST_EXPR(false == 0.0, false);
        TEST_EXPR(false == 1.0, false);
        TEST_EXPR(false != 0.0, true);
        TEST_EXPR(false != 1.0, true);
        TEST_EXPR(false > 0.0, false);
        TEST_EXPR(false >= 0.0, false);
        TEST_EXPR(false < 0.0, true);
        TEST_EXPR(false <= 0.0, true);

        TEST_EXPR(true == 1, false);
        TEST_EXPR(true == 2, false);
        TEST_EXPR(true != 1, true);
        TEST_EXPR(true != 2, true);
        TEST_EXPR(true > 1, false);
        TEST_EXPR(true >= 1, false);
        TEST_EXPR(true < 1, true);
        TEST_EXPR(true <= 1, true);
        TEST_EXPR(false == 0, false);
        TEST_EXPR(false == 1, false);
        TEST_EXPR(false != 0, true);
        TEST_EXPR(false != 1, true);
        TEST_EXPR(false > 0, false);
        TEST_EXPR(false >= 0, false);
        TEST_EXPR(false < 0, true);
        TEST_EXPR(false <= 0, true);
    }
    {
        TEST_EXPR(-1 == -2, false);
        TEST_EXPR(-2 == -1, false);
        TEST_EXPR(-1 != -2, true);
        TEST_EXPR(-2 != -1, true);
        TEST_EXPR(-1 > -2, true);
        TEST_EXPR(-2 > -1, false);
        TEST_EXPR(-1 >= -2, true);
        TEST_EXPR(-2 >= -1, false);
        TEST_EXPR(-1 < -2, false);
        TEST_EXPR(-2 < -1, true);
        TEST_EXPR(-1 <= -2, false);
        TEST_EXPR(-2 <= -1, true);

        TEST_EXPR(0.5 == 1, false);
        TEST_EXPR(1.0 == 1, true);
        TEST_EXPR(0.5 != 1, true);
        TEST_EXPR(1.0 != 1, false);
        TEST_EXPR(0.5 > 1, false);
        TEST_EXPR(0.5 >= 1, false);
        TEST_EXPR(0.5 < 1, true);
        TEST_EXPR(0.5 <= 1, true);

        TEST_EXPR(-1 == -1, true);
        TEST_EXPR(-1 != -1, false);
        TEST_EXPR(-1 > -1, false);
        TEST_EXPR(-1 >= -1, true);
        TEST_EXPR(-1 < -1, false);
        TEST_EXPR(-1 <= -1, true);

        TEST_EXPR(1 == 2, false);
        TEST_EXPR(2 == 1, false);
        TEST_EXPR(1 != 2, true);
        TEST_EXPR(2 != 1, true);
        TEST_EXPR(1 > 2, false);
        TEST_EXPR(2 > 1, true);
        TEST_EXPR(1 >= 2, false);
        TEST_EXPR(2 >= 1, true);
        TEST_EXPR(1 < 2, true);
        TEST_EXPR(2 < 1, false);
        TEST_EXPR(1 <= 2, true);
        TEST_EXPR(2 <= 1, false);

        TEST_EXPR(1 == 1, true);
        TEST_EXPR(1 != 1, false);
        TEST_EXPR(1 > 1, false);
        TEST_EXPR(1 >= 1, true);
        TEST_EXPR(1 < 1, false);
        TEST_EXPR(1 <= 1, true);
    }
    {
        TEST_EXPR(8 % 2 + 1 == 1, true);
        TEST_EXPR(8 % 2 + 1 != 1, false);
        TEST_EXPR(8 % 3 + 1 == 3, true);
        TEST_EXPR(8 % 3 + 1 != 3, false);
        TEST_EXPR(8 % 3 > 1, true);
        TEST_EXPR(8 % 3 >= 2, true);
        TEST_EXPR(8 % 3 <= 2, true);
        TEST_EXPR(8 % 3 < 2, false);

        TEST_EXPR(3.14 * 3 * 3 / 2 > 3.14 * 1.5 * 1.5 / 2, true);
        TEST_EXPR(3.14 * 3 * 3 / 2 < 3.14 * 1.5 * 1.5 / 2, false);
    }
}


TEST_F(ExpressionTest, FunctionCallTest) {
    {
        TEST_FUNCTION(abs, args_["neg_int"], 1);
        TEST_FUNCTION(abs, args_["neg_float"], 1.1);
        TEST_FUNCTION(abs, args_["int"], 4);
        TEST_FUNCTION(abs, args_["float"], 1.1);
    }
    {
        TEST_FUNCTION(floor, args_["neg_int"], -1);
        TEST_FUNCTION(floor, args_["float"], 1);
        TEST_FUNCTION(floor, args_["neg_float"], -2);
        TEST_FUNCTION(floor, args_["int"], 4);
    }
    {
        TEST_FUNCTION(sqrt, args_["int"], 2);
        TEST_FUNCTION(sqrt, args_["float"], std::sqrt(1.1));
    }
    {
        TEST_FUNCTION(pow, args_["pow"], 8);
        TEST_FUNCTION(exp, args_["int"], std::exp(4));
        TEST_FUNCTION(exp2, args_["int"], 16);

        TEST_FUNCTION(log, args_["int"], std::log(4));
        TEST_FUNCTION(log2, args_["int"], 2);
    }
    {
        TEST_FUNCTION(lower, args_["string"], "abcdefg");
        TEST_FUNCTION(upper, args_["string"], "ABCDEFG");
        TEST_FUNCTION(length, args_["string"], 7);

        TEST_FUNCTION(trim, args_["trim"], "abc");
        TEST_FUNCTION(ltrim, args_["trim"], "abc  ");
        TEST_FUNCTION(rtrim, args_["trim"], " abc");
    }
    {
        TEST_FUNCTION(substr, args_["substr"], "bcde");
        TEST_FUNCTION(left, args_["side"], "abcde");
        TEST_FUNCTION(right, args_["side"], "mnopq");
        TEST_FUNCTION(left, args_["neg_side"], "");
        TEST_FUNCTION(right, args_["neg_side"], "");

        TEST_FUNCTION(lpad, args_["pad"], "1231abcdefghijkl");
        TEST_FUNCTION(rpad, args_["pad"], "abcdefghijkl1231");
        TEST_FUNCTION(udf_is_in, args_["udf_is_in"], true);
    }
}

TEST_F(ExpressionTest, Arithmetics) {
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
    {
        // $^.source.string16 + $$.dest.string16
        ArithmeticExpression add(
            Expression::Kind::kAdd,
            new SourcePropertyExpression(new std::string("source"), new std::string("string16")),
            new DestPropertyExpression(new std::string("dest"), new std::string("string16")));
        auto eval = Expression::eval(&add, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, std::string(32, 'a'));
    }
    {
        // 10 - e1.int
        ArithmeticExpression minus(
            Expression::Kind::kMinus,
            new ConstantExpression(10),
            new EdgePropertyExpression(new std::string("e1"), new std::string("int")));
        auto eval = Expression::eval(&minus, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 9);
    }
    {
        // 10 - $^.source.int
        ArithmeticExpression minus(
            Expression::Kind::kMinus,
            new ConstantExpression(10),
            new SourcePropertyExpression(new std::string("source"), new std::string("int")));
        auto eval = Expression::eval(&minus, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 9);
    }
    {
        // e1.string128 - e1.string64
        ArithmeticExpression minus(
            Expression::Kind::kMinus,
            new EdgePropertyExpression(new std::string("e1"), new std::string("string128")),
            new EdgePropertyExpression(new std::string("e1"), new std::string("string64")));
        auto eval = Expression::eval(&minus, gExpCtxt);
        EXPECT_NE(eval.type(), Value::Type::STRING);
        EXPECT_NE(eval, std::string(64, 'a'));
    }
    {
        // $^.source.srcProperty % $$.dest.dstProperty
        ArithmeticExpression mod(
            Expression::Kind::kMod,
            new SourcePropertyExpression(new std::string("source"), new std::string("srcProperty")),
            new DestPropertyExpression(new std::string("dest"), new std::string("dstProperty")));
        auto eval = Expression::eval(&mod, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
}

TEST_F(ExpressionTest, Relation) {
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
    {
        // 1 == NULL
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new ConstantExpression(Value(1)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // NULL == NULL
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new ConstantExpression(Value(NullType::NaN)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // 1 != NULL
        RelationalExpression expr(
                Expression::Kind::kRelNE,
                new ConstantExpression(Value(1)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // NULL != NULL
        RelationalExpression expr(
                Expression::Kind::kRelNE,
                new ConstantExpression(Value(NullType::NaN)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // 1 < NULL
        RelationalExpression expr(
                Expression::Kind::kRelLT,
                new ConstantExpression(Value(1)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullValue);
    }
    {
        // NULL < NULL
        RelationalExpression expr(
                Expression::Kind::kRelLT,
                new ConstantExpression(Value(NullType::NaN)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullValue);
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

TEST_F(ExpressionTest, toStringTest) {
    {
        ConstantExpression ep(1);
        EXPECT_EQ(ep.toString(), "1");
    }
    {
        ConstantExpression ep(1.123);
        EXPECT_EQ(ep.toString(), "1.123");
    }
    // FIXME: double/float to string conversion
    // {
    //     ConstantExpression ep(1.0);
    //     EXPECT_EQ(ep.toString(), "1.0");
    // }
    {
        ConstantExpression ep(true);
        EXPECT_EQ(ep.toString(), "true");
    }
    {
        ConstantExpression ep(List(std::vector<Value>{1, 2, 3, 4, 9, 0, -23}));
        EXPECT_EQ(ep.toString(), "[1,2,3,4,9,0,-23]");
    }
    {
        ConstantExpression ep(Map({{"hello", "world"}, {"name", "zhang"}}));
        EXPECT_EQ(ep.toString(), "{\"name\":zhang,\"hello\":world}");
    }
    {
        ConstantExpression ep(Set({1, 2.3, "hello", true}));
        EXPECT_EQ(ep.toString(), "{hello,2.3,true,1}");
    }
    {
        ConstantExpression ep(Date(1234));
        EXPECT_EQ(ep.toString(), "-32765/05/19");
    }
    {
        ConstantExpression ep(Edge("100", "102", 2, "like", 3, {{"likeness", 95}}));
        EXPECT_EQ(ep.toString(), "(100)-[like]->(102)@3 likeness:95");
    }
    {
        ConstantExpression ep(Vertex("100", {Tag("player", {{"name", "jame"}})}));
        EXPECT_EQ(ep.toString(), "(100) Tag: player, name:jame");
    }
    {
        TypeCastingExpression ep(Value::Type::FLOAT, new ConstantExpression(2));
        EXPECT_EQ(ep.toString(), "(FLOAT)2");
    }
    {
        UnaryExpression plus(Expression::Kind::kUnaryPlus, new ConstantExpression(2));
        EXPECT_EQ(plus.toString(), "+(2)");

        UnaryExpression nega(Expression::Kind::kUnaryNegate, new ConstantExpression(2));
        EXPECT_EQ(nega.toString(), "-(2)");

        UnaryExpression incr(Expression::Kind::kUnaryIncr, new ConstantExpression(2));
        EXPECT_EQ(incr.toString(), "++(2)");

        UnaryExpression decr(Expression::Kind::kUnaryDecr, new ConstantExpression(2));
        EXPECT_EQ(decr.toString(), "--(2)");

        UnaryExpression no(Expression::Kind::kUnaryNot, new ConstantExpression(2));
        EXPECT_EQ(no.toString(), "!(2)");
    }
    {
        VariableExpression var(new std::string("name"));
        EXPECT_EQ(var.toString(), "$name");

        VersionedVariableExpression versionVar(new std::string("name"), new ConstantExpression(1));
        EXPECT_EQ(versionVar.toString(), "$name{1}");
    }
    {
        TEST_TOSTRING(2 + 2 - 3, "((2+2)-3)");
        TEST_TOSTRING(true || true, "(true||true)");
        TEST_TOSTRING(true && false || false, "((true&&false)||false)");
        TEST_TOSTRING(true == 2, "(true==2)");
        TEST_TOSTRING(2 > 1 && 3 > 2, "((2>1)&&(3>2))");
        TEST_TOSTRING((3 + 5) * 3 / (6 - 2), "(((3+5)*3)/(6-2))");
        TEST_TOSTRING(76 - 100 / 20 * 4, "(76-((100/20)*4))");
        TEST_TOSTRING(8 % 2 + 1 == 1, "(((8%2)+1)==1)");
        TEST_TOSTRING(1 == 2, "(1==2)");
    }
}

TEST_F(ExpressionTest, FunctionCallToStringTest) {
    {
        ArgumentList *argList = new ArgumentList();
        for (const auto &i : args_["pow"]) {
            argList->addArgument(std::make_unique<ConstantExpression>(std::move(i)));
        }
        FunctionCallExpression ep(new std::string("pow"), argList);
        EXPECT_EQ(ep.toString(), "pow(2,3)");
    }
    {
        ArgumentList *argList = new ArgumentList();
        for (const auto &i : args_["udf_is_in"]) {
            argList->addArgument(std::make_unique<ConstantExpression>(std::move(i)));
        }
        FunctionCallExpression ep(new std::string("udf_is_in"), argList);
        EXPECT_EQ(ep.toString(), "udf_is_in(4,1,2,8,4,3,1,0)");
    }
    {
        ArgumentList *argList = new ArgumentList();
        for (const auto &i : args_["neg_int"]) {
            argList->addArgument(std::make_unique<ConstantExpression>(std::move(i)));
        }
        FunctionCallExpression ep(new std::string("abs"), argList);
        EXPECT_EQ(ep.toString(), "abs(-1)");
    }
    {
        FunctionCallExpression ep(new std::string("now"));
        EXPECT_EQ(ep.toString(), "now()");
    }
}

TEST_F(ExpressionTest, PropertyToStringTest) {
    {
        SymbolPropertyExpression ep(Expression::Kind::kSymProperty,
                                    nullptr,
                                    new std::string("like"),
                                    new std::string("likeness"));
        EXPECT_EQ(ep.toString(), "like.likeness");
    }
    {
        SymbolPropertyExpression ep(Expression::Kind::kSymProperty,
                                    new std::string("$$"),
                                    new std::string("like"),
                                    new std::string("likeness"));
        EXPECT_EQ(ep.toString(), "$$.like.likeness");
    }
    {
        EdgePropertyExpression ep(new std::string("like"), new std::string("likeness"));
        EXPECT_EQ(ep.toString(), "like.likeness");
    }
    {
        InputPropertyExpression ep(new std::string("name"));
        EXPECT_EQ(ep.toString(), "$-.name");
    }
    {
        VariablePropertyExpression ep(new std::string("player"), new std::string("name"));
        EXPECT_EQ(ep.toString(), "$player.name");
    }
    {
        SourcePropertyExpression ep(new std::string("player"), new std::string("age"));
        EXPECT_EQ(ep.toString(), "$^.player.age");
    }
    {
        DestPropertyExpression ep(new std::string("player"), new std::string("age"));
        EXPECT_EQ(ep.toString(), "$$.player.age");
    }
    {
        EdgeSrcIdExpression ep(new std::string("like"));
        EXPECT_EQ(ep.toString(), "like._src");
    }
    {
        EdgeTypeExpression ep(new std::string("like"));
        EXPECT_EQ(ep.toString(), "like._type");
    }
    {
        EdgeRankExpression ep(new std::string("like"));
        EXPECT_EQ(ep.toString(), "like._rank");
    }
    {
        EdgeDstIdExpression ep(new std::string("like"));
        EXPECT_EQ(ep.toString(), "like._dst");
    }
}

TEST_F(ExpressionTest, ListToString) {
    auto *elist = new ExpressionList();
    (*elist).add(new ConstantExpression(12345))
            .add(new ConstantExpression("Hello"))
            .add(new ConstantExpression(true));
    auto expr = std::make_unique<ListExpression>(elist);
    ASSERT_EQ("[12345,Hello,true]", expr->toString());
}

TEST_F(ExpressionTest, SetToString) {
    auto *elist = new ExpressionList();
    (*elist).add(new ConstantExpression(12345))
            .add(new ConstantExpression(12345))
            .add(new ConstantExpression("Hello"))
            .add(new ConstantExpression(true));
    auto expr = std::make_unique<SetExpression>(elist);
    ASSERT_EQ("{12345,12345,Hello,true}", expr->toString());
}

TEST_F(ExpressionTest, MapTostring) {
    auto *items = new MapItemList();
    (*items).add(new std::string("key1"), new ConstantExpression(12345))
            .add(new std::string("key2"), new ConstantExpression(12345))
            .add(new std::string("key3"), new ConstantExpression("Hello"))
            .add(new std::string("key4"), new ConstantExpression(true));
    auto expr = std::make_unique<MapExpression>(items);
    auto expected = "{"
                        "\"key1\":12345,"
                        "\"key2\":12345,"
                        "\"key3\":Hello,"
                        "\"key4\":true"
                    "}";
    ASSERT_EQ(expected, expr->toString());
}

TEST_F(ExpressionTest, ListEvaluate) {
    auto *elist = new ExpressionList();
    (*elist).add(new ConstantExpression(12345))
            .add(new ConstantExpression("Hello"))
            .add(new ConstantExpression(true));
    auto expr = std::make_unique<ListExpression>(elist);
    auto expected = Value(List({12345, "Hello", true}));
    auto value = Expression::eval(expr.get(), gExpCtxt);
    ASSERT_EQ(expected, value);
}

TEST_F(ExpressionTest, SetEvaluate) {
    auto *elist = new ExpressionList();
    (*elist).add(new ConstantExpression(12345))
            .add(new ConstantExpression(12345))
            .add(new ConstantExpression("Hello"))
            .add(new ConstantExpression(true));
    auto expr = std::make_unique<SetExpression>(elist);
    auto expected = Value(Set({12345, "Hello", true}));
    auto value = Expression::eval(expr.get(), gExpCtxt);
    ASSERT_EQ(expected, value);
}

TEST_F(ExpressionTest, MapEvaluate) {
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(12345))
                .add(new std::string("key2"), new ConstantExpression(12345))
                .add(new std::string("key3"), new ConstantExpression("Hello"))
                .add(new std::string("key4"), new ConstantExpression(true));
        auto expr = std::make_unique<MapExpression>(items);
        auto expected = Value(Map({
                                    {"key1", 12345},
                                    {"key2", 12345},
                                    {"key3", "Hello"},
                                    {"key4", true}}));
        auto value = Expression::eval(expr.get(), gExpCtxt);
        ASSERT_EQ(expected, value);
    }
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(12345))
                .add(new std::string("key2"), new ConstantExpression(12345))
                .add(new std::string("key3"), new ConstantExpression("Hello"))
                .add(new std::string("key4"), new ConstantExpression(false))
                .add(new std::string("key4"), new ConstantExpression(true));
        auto expr = std::make_unique<MapExpression>(items);
        auto expected = Value(Map({
                                    {"key1", 12345},
                                    {"key2", 12345},
                                    {"key3", "Hello"},
                                    {"key4", false}}));
        auto value = Expression::eval(expr.get(), gExpCtxt);
        ASSERT_EQ(expected, value);
    }
}

TEST_F(ExpressionTest, InList) {
    {
        auto *elist = new ExpressionList;
        (*elist).add(new ConstantExpression(12345))
                .add(new ConstantExpression("Hello"))
                .add(new ConstantExpression(true));
        auto listExpr = new ListExpression(elist);
        RelationalExpression expr(Expression::Kind::kRelIn,
                                  new ConstantExpression(12345),
                                  listExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
    {
        auto *elist = new ExpressionList;
        (*elist).add(new ConstantExpression(12345))
                .add(new ConstantExpression("Hello"))
                .add(new ConstantExpression(true));
        auto listExpr = new ListExpression(elist);
        RelationalExpression expr(Expression::Kind::kRelIn,
                                  new ConstantExpression(false),
                                  listExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
}

TEST_F(ExpressionTest, InSet) {
    {
        auto *elist = new ExpressionList;
        (*elist).add(new ConstantExpression(12345))
                .add(new ConstantExpression("Hello"))
                .add(new ConstantExpression(true));
        auto setExpr = new SetExpression(elist);
        RelationalExpression expr(Expression::Kind::kRelIn,
                                  new ConstantExpression(12345),
                                  setExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
    {
        auto *elist = new ExpressionList;
        (*elist).add(new ConstantExpression(12345))
                .add(new ConstantExpression("Hello"))
                .add(new ConstantExpression(true));
        auto setExpr = new ListExpression(elist);
        RelationalExpression expr(Expression::Kind::kRelIn,
                                  new ConstantExpression(false),
                                  setExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
}

TEST_F(ExpressionTest, InMap) {
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(12345))
                .add(new std::string("key2"), new ConstantExpression(12345))
                .add(new std::string("key3"), new ConstantExpression("Hello"))
                .add(new std::string("key4"), new ConstantExpression(true));
        auto mapExpr = new MapExpression(items);
        RelationalExpression expr(Expression::Kind::kRelIn,
                                  new ConstantExpression("key1"),
                                  mapExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(12345))
                .add(new std::string("key2"), new ConstantExpression(12345))
                .add(new std::string("key3"), new ConstantExpression("Hello"))
                .add(new std::string("key4"), new ConstantExpression(true));
        auto mapExpr = new MapExpression(items);
        RelationalExpression expr(Expression::Kind::kRelIn,
                                  new ConstantExpression("key5"),
                                  mapExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(12345))
                .add(new std::string("key2"), new ConstantExpression(12345))
                .add(new std::string("key3"), new ConstantExpression("Hello"))
                .add(new std::string("key4"), new ConstantExpression(true));
        auto mapExpr = new MapExpression(items);
        RelationalExpression expr(Expression::Kind::kRelIn,
                                  new ConstantExpression(12345),
                                  mapExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
}

TEST_F(ExpressionTest, NotInList) {
    {
        auto *elist = new ExpressionList;
        (*elist).add(new ConstantExpression(12345))
                .add(new ConstantExpression("Hello"))
                .add(new ConstantExpression(true));
        auto listExpr = new ListExpression(elist);
        RelationalExpression expr(Expression::Kind::kRelNotIn,
                                  new ConstantExpression(12345),
                                  listExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
    {
        auto *elist = new ExpressionList;
        (*elist).add(new ConstantExpression(12345))
                .add(new ConstantExpression("Hello"))
                .add(new ConstantExpression(true));
        auto listExpr = new ListExpression(elist);
        RelationalExpression expr(Expression::Kind::kRelNotIn,
                                  new ConstantExpression(false),
                                  listExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
}

TEST_F(ExpressionTest, NotInSet) {
    {
        auto *elist = new ExpressionList;
        (*elist).add(new ConstantExpression(12345))
                .add(new ConstantExpression("Hello"))
                .add(new ConstantExpression(true));
        auto setExpr = new SetExpression(elist);
        RelationalExpression expr(Expression::Kind::kRelNotIn,
                                  new ConstantExpression(12345),
                                  setExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
    {
        auto *elist = new ExpressionList;
        (*elist).add(new ConstantExpression(12345))
                .add(new ConstantExpression("Hello"))
                .add(new ConstantExpression(true));
        auto setExpr = new ListExpression(elist);
        RelationalExpression expr(Expression::Kind::kRelNotIn,
                                  new ConstantExpression(false),
                                  setExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
}

TEST_F(ExpressionTest, NotInMap) {
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(12345))
                .add(new std::string("key2"), new ConstantExpression(12345))
                .add(new std::string("key3"), new ConstantExpression("Hello"))
                .add(new std::string("key4"), new ConstantExpression(true));
        auto mapExpr = new MapExpression(items);
        RelationalExpression expr(Expression::Kind::kRelNotIn,
                                  new ConstantExpression("key1"),
                                  mapExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value);
    }
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(12345))
                .add(new std::string("key2"), new ConstantExpression(12345))
                .add(new std::string("key3"), new ConstantExpression("Hello"))
                .add(new std::string("key4"), new ConstantExpression(true));
        auto mapExpr = new MapExpression(items);
        RelationalExpression expr(Expression::Kind::kRelNotIn,
                                  new ConstantExpression("key5"),
                                  mapExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(12345))
                .add(new std::string("key2"), new ConstantExpression(12345))
                .add(new std::string("key3"), new ConstantExpression("Hello"))
                .add(new std::string("key4"), new ConstantExpression(true));
        auto mapExpr = new MapExpression(items);
        RelationalExpression expr(Expression::Kind::kRelNotIn,
                                  new ConstantExpression(12345),
                                  mapExpr);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value);
    }
}

TEST_F(ExpressionTest, ListSubscript) {
    // [1,2,3,4][0]
    {
        auto *items = new ExpressionList();
        (*items).add(new ConstantExpression(1))
                .add(new ConstantExpression(2))
                .add(new ConstantExpression(3))
                .add(new ConstantExpression(4));
        auto *list = new ListExpression(items);
        auto *index = new ConstantExpression(0);
        SubscriptExpression expr(list, index);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
    // [1,2,3,4][3]
    {
        auto *items = new ExpressionList();
        (*items).add(new ConstantExpression(1))
                .add(new ConstantExpression(2))
                .add(new ConstantExpression(3))
                .add(new ConstantExpression(4));
        auto *list = new ListExpression(items);
        auto *index = new ConstantExpression(3);
        SubscriptExpression expr(list, index);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(4, value.getInt());
    }
    // [1,2,3,4][4]
    {
        auto *items = new ExpressionList();
        (*items).add(new ConstantExpression(1))
                .add(new ConstantExpression(2))
                .add(new ConstantExpression(3))
                .add(new ConstantExpression(4));
        auto *list = new ListExpression(items);
        auto *index = new ConstantExpression(4);
        SubscriptExpression expr(list, index);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
    // [1,2,3,4][-1]
    {
        auto *items = new ExpressionList();
        (*items).add(new ConstantExpression(1))
                .add(new ConstantExpression(2))
                .add(new ConstantExpression(3))
                .add(new ConstantExpression(4));
        auto *list = new ListExpression(items);
        auto *index = new ConstantExpression(-1);
        SubscriptExpression expr(list, index);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(4, value.getInt());
    }
    // [1,2,3,4][-4]
    {
        auto *items = new ExpressionList();
        (*items).add(new ConstantExpression(1))
                .add(new ConstantExpression(2))
                .add(new ConstantExpression(3))
                .add(new ConstantExpression(4));
        auto *list = new ListExpression(items);
        auto *index = new ConstantExpression(-4);
        SubscriptExpression expr(list, index);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
    // [1,2,3,4][-5]
    {
        auto *items = new ExpressionList();
        (*items).add(new ConstantExpression(1))
                .add(new ConstantExpression(2))
                .add(new ConstantExpression(3))
                .add(new ConstantExpression(4));
        auto *list = new ListExpression(items);
        auto *index = new ConstantExpression(-5);
        SubscriptExpression expr(list, index);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
    // [1,2,3,4]["0"]
    {
        auto *items = new ExpressionList();
        (*items).add(new ConstantExpression(1))
                .add(new ConstantExpression(2))
                .add(new ConstantExpression(3))
                .add(new ConstantExpression(4));
        auto *list = new ListExpression(items);
        auto *index = new ConstantExpression("0");
        SubscriptExpression expr(list, index);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
    // 1[0]
    {
        auto *integer = new ConstantExpression(1);
        auto *index = new ConstantExpression(0);
        SubscriptExpression expr(integer, index);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
}

TEST_F(ExpressionTest, MapSubscript) {
    // {"key1":1,"key2":2, "key3":3}["key1"]
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(1))
                .add(new std::string("key2"), new ConstantExpression(2))
                .add(new std::string("key3"), new ConstantExpression(3));
        auto *map = new MapExpression(items);
        auto *key = new ConstantExpression("key1");
        SubscriptExpression expr(map, key);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
    // {"key1":1,"key2":2, "key3":3}["key4"]
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(1))
                .add(new std::string("key2"), new ConstantExpression(2))
                .add(new std::string("key3"), new ConstantExpression(3));
        auto *map = new MapExpression(items);
        auto *key = new ConstantExpression("key4");
        SubscriptExpression expr(map, key);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isNull());
        ASSERT_FALSE(value.isBadNull());
    }
    // {"key1":1,"key2":2, "key3":3}[0]
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(1))
                .add(new std::string("key2"), new ConstantExpression(2))
                .add(new std::string("key3"), new ConstantExpression(3));
        auto *map = new MapExpression(items);
        auto *key = new ConstantExpression(0);
        SubscriptExpression expr(map, key);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isNull());
        ASSERT_FALSE(value.isBadNull());
    }
}

TEST_F(ExpressionTest, TypeCastTest) {
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression(1));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression(1.23));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression("1.23"));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression("123"));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 123);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression(".123"));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 0);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression(".123ab"));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression("abc123"));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression("123abc"));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression(true));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }

    {
        TypeCastingExpression typeCast(Value::Type::FLOAT, new ConstantExpression(1.23));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 1.23);
    }
    {
        TypeCastingExpression typeCast(Value::Type::FLOAT, new ConstantExpression(2));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 2.0);
    }
    {
        TypeCastingExpression typeCast(Value::Type::FLOAT, new ConstantExpression("1.23"));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::FLOAT);
        EXPECT_EQ(eval, 1.23);
    }
    {
        TypeCastingExpression typeCast(Value::Type::BOOL, new ConstantExpression(2));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        TypeCastingExpression typeCast(Value::Type::BOOL, new ConstantExpression(0));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        TypeCastingExpression typeCast(Value::Type::STRING, new ConstantExpression(true));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "true");
    }
    {
        TypeCastingExpression typeCast(Value::Type::STRING, new ConstantExpression(false));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "false");
    }
    {
        TypeCastingExpression typeCast(Value::Type::STRING, new ConstantExpression(12345));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "12345");
    }
    {
        TypeCastingExpression typeCast(Value::Type::STRING, new ConstantExpression(34.23));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "34.23");
    }
    {
        TypeCastingExpression typeCast(Value::Type::STRING, new ConstantExpression(.23));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::STRING);
        EXPECT_EQ(eval, "0.23");
    }
    {
        TypeCastingExpression typeCast(Value::Type::SET, new ConstantExpression(23));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        TypeCastingExpression typeCast(Value::Type::INT, new ConstantExpression(Set()));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
}

TEST_F(ExpressionTest, RelationContains) {
    {
        // "abc" contains "a"
        RelationalExpression expr(
                Expression::Kind::kContains,
                new ConstantExpression("abc"),
                new ConstantExpression("a"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" contains "bc"
        RelationalExpression expr(
                Expression::Kind::kContains,
                new ConstantExpression("abc"),
                new ConstantExpression("bc"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" contains "d"
        RelationalExpression expr(
                Expression::Kind::kContains,
                new ConstantExpression("abc"),
                new ConstantExpression("d"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" contains 1
        RelationalExpression expr(
                Expression::Kind::kContains,
                new ConstantExpression("abc1"),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 contains 1
        RelationalExpression expr(
                Expression::Kind::kContains,
                new ConstantExpression(1234),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
}

TEST_F(ExpressionTest, ContainsToString) {
    {
        // "abc" contains "a"
        RelationalExpression expr(
                Expression::Kind::kContains,
                new ConstantExpression("abc"),
                new ConstantExpression("a"));
        ASSERT_EQ("(abc CONTAINS a)", expr.toString());
    }
}

TEST_F(ExpressionTest, LabelExprToString) {
    LabelExpression expr(new std::string("name"));
    ASSERT_EQ("name", expr.toString());
}
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
