/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Vertex.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/AttributeExpression.h"
#include "common/expression/AggregateExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/EdgeExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PathBuildExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/VertexExpression.h"
#include "common/expression/CaseExpression.h"
#include "common/expression/ColumnExpression.h"
#include "common/expression/ListComprehensionExpression.h"
#include "common/expression/PredicateExpression.h"
#include "common/expression/ReduceExpression.h"
#include "common/expression/test/ExpressionContextMock.h"

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
    std::unordered_map<std::string, int8_t> priority = {{"OR", 1},
                                                        {"AND", 2},
                                                        {"XOR", 3},
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
        std::vector<std::string> logicalOp = {"AND", "OR", "XOR"};
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
        EXPECT_EQ(eval.type(), expected.type()) << "type check failed: " << ep->toString();
        EXPECT_EQ(eval, expected) << "check failed: " << ep->toString();
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

    void testAggExpr(const char* name,
                     bool isDistinct,
                     const char* expr,
                     std::vector<std::pair<std::string, Value>> inputVar,
                     const std::unordered_map<std::string, Value> &expected) {
        auto agg = new std::string(name);
        auto func = std::make_unique<std::string>(expr);
        Expression* arg = nullptr;
        auto isConst = false;
        if (!func->compare("isConst")) {
            isConst = true;
            arg = new ConstantExpression();
        } else {
            arg = new FunctionCallExpression(func.release());
        }
        AggregateExpression aggExpr(agg, arg, isDistinct);
        std::unordered_map<std::string, std::unique_ptr<AggData>> agg_data_map;
        for (const auto &row : inputVar) {
            auto iter = agg_data_map.find(row.first);
            if (iter == agg_data_map.end()) {
                agg_data_map[row.first] = std::make_unique<AggData>();
            }
            if (isConst) {
                static_cast<ConstantExpression*>(arg)->setValue(row.second);
            } else {
                auto args = std::make_unique<ArgumentList>(1);
                args->addArgument(std::make_unique<ConstantExpression>(row.second));
                static_cast<FunctionCallExpression*>(arg)->setArgs(std::move(args).release());
            }
            aggExpr.setAggData(agg_data_map[row.first].get());
            auto eval = aggExpr.eval(gExpCtxt);
        }
        std::unordered_map<std::string, Value> res;
        for (auto& iter : agg_data_map) {
            res[iter.first] = iter.second->result();
        }
        EXPECT_EQ(res, expected) << "check failed: " << name;
    }
};

std::unordered_map<std::string, Expression::Kind> ExpressionTest::op_ = {
    {"+", Expression::Kind::kAdd},
    {"-", Expression::Kind::kMinus},
    {"*", Expression::Kind::kMultiply},
    {"/", Expression::Kind::kDivision},
    {"%", Expression::Kind::kMod},
    {"OR", Expression::Kind::kLogicalOr},
    {"AND", Expression::Kind::kLogicalAnd},
    {"XOR", Expression::Kind::kLogicalXor},
    {">", Expression::Kind::kRelGT},
    {"<", Expression::Kind::kRelLT},
    {">=", Expression::Kind::kRelGE},
    {"<=", Expression::Kind::kRelLE},
    {"==", Expression::Kind::kRelEQ},
    {"!=", Expression::Kind::kRelNE},
    {"!", Expression::Kind::kUnaryNot}};

std::unordered_map<std::string, Value>
    ExpressionTest::boolen_ = {{"true", Value(true)},
                               {"false", Value(false)},
                               {"empty", Value()},
                               {"null", Value(NullType::__NULL__)}};

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
    } while (0)

#define TEST_FUNCTION(expr, args, expected)                                                        \
    do {                                                                                           \
        testFunction(#expr, args, expected);                                                       \
    } while (0)

#define TEST_AGG(name, isDistinct, expr, inputVar, expected)                                      \
    do {                                                                                          \
        testAggExpr(#name,                                                                        \
                    isDistinct,                                                                   \
                    #expr,                                                                        \
                    inputVar,                                                                     \
                    expected);                                                                    \
    } while (0)

#define TEST_TOSTRING(expr, expected)                                                              \
    do {                                                                                           \
        testToString(#expr, expected);                                                             \
    } while (0)

#define STEP(DST, NAME, RANKING, TYPE)                                                             \
    do {                                                                                           \
        Step step;                                                                                 \
        step.dst.vid = DST;                                                                        \
        step.name = NAME;                                                                          \
        step.ranking = RANKING;                                                                    \
        step.type = TYPE;                                                                          \
        path.steps.emplace_back(std::move(step));                                                  \
    } while (0)

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
        Time time;
        time.hour = 3;
        time.minute = 33;
        time.sec = 3;
        ConstantExpression timeExpr(time);
        auto eval = Expression::eval(&timeExpr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::TIME);
        EXPECT_EQ(eval, time);
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
        TEST_EXPR(true XOR true, false);
        TEST_EXPR(true XOR false, true);
        TEST_EXPR(false XOR false, false);
        TEST_EXPR(false XOR true, true);
    }
    {
        TEST_EXPR(true AND true AND true, true);
        TEST_EXPR(true AND true OR false, true);
        TEST_EXPR(true AND true AND false, false);
        TEST_EXPR(true OR false AND true OR false, true);
        TEST_EXPR(true XOR true XOR false, false);
    }
    {
        // AND
        TEST_EXPR(true AND true, true);
        TEST_EXPR(true AND false, false);
        TEST_EXPR(false AND true, false);
        TEST_EXPR(false AND false, false);

        // AND AND  ===  (AND) AND
        TEST_EXPR(true AND true AND true, true);
        TEST_EXPR(true AND true AND false, false);
        TEST_EXPR(true AND false AND true, false);
        TEST_EXPR(true AND false AND false, false);
        TEST_EXPR(false AND true AND true, false);
        TEST_EXPR(false AND true AND false, false);
        TEST_EXPR(false AND false AND true, false);
        TEST_EXPR(false AND false AND false, false);

        // OR
        TEST_EXPR(true OR true, true);
        TEST_EXPR(true OR false, true);
        TEST_EXPR(false OR true, true);
        TEST_EXPR(false OR false, false);

        // OR OR  ===  (OR) OR
        TEST_EXPR(true OR true OR true, true);
        TEST_EXPR(true OR true OR false, true);
        TEST_EXPR(true OR false OR true, true);
        TEST_EXPR(true OR false OR false, true);
        TEST_EXPR(false OR true OR true, true);
        TEST_EXPR(false OR true OR false, true);
        TEST_EXPR(false OR false OR true, true);
        TEST_EXPR(false OR false OR false, false);

        // AND OR  ===  (AND) OR
        TEST_EXPR(true AND true OR true, true);
        TEST_EXPR(true AND true OR false, true);
        TEST_EXPR(true AND false OR true, true);
        TEST_EXPR(true AND false OR false, false);
        TEST_EXPR(false AND true OR true, true);
        TEST_EXPR(false AND true OR false, false);
        TEST_EXPR(false AND false OR true, true);
        TEST_EXPR(false AND false OR false, false);

        // OR AND  === OR (AND)
        TEST_EXPR(true OR true AND true, true);
        TEST_EXPR(true OR true AND false, true);
        TEST_EXPR(true OR false AND true, true);
        TEST_EXPR(true OR false AND false, true);
        TEST_EXPR(false OR true AND true, true);
        TEST_EXPR(false OR true AND false, false);
        TEST_EXPR(false OR false AND true, false);
        TEST_EXPR(false OR false AND false, false);
    }
    {
        TEST_EXPR(2 > 1 AND 3 > 2, true);
        TEST_EXPR(2 <= 1 AND 3 > 2, false);
        TEST_EXPR(2 > 1 AND 3 < 2, false);
        TEST_EXPR(2 < 1 AND 3 < 2, false);
    }
    {
        // test bad null
        TEST_EXPR(2 / 0, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 AND true, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 AND false, Value::Value::kNullDivByZero);
        TEST_EXPR(true AND 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(false AND 2 / 0, false);
        TEST_EXPR(2 / 0 AND 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(empty AND null AND 2 / 0 AND empty, Value::kNullDivByZero);

        TEST_EXPR(2 / 0 OR true, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 OR false, Value::kNullDivByZero);
        TEST_EXPR(true OR 2 / 0, true);
        TEST_EXPR(false OR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 OR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(empty OR null OR 2 / 0 OR empty, Value::kNullDivByZero);

        TEST_EXPR(2 / 0 XOR true, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 XOR false, Value::kNullDivByZero);
        TEST_EXPR(true XOR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(false XOR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 XOR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(empty XOR 2 / 0 XOR null XOR empty, Value::kNullDivByZero);

        // test normal null
        TEST_EXPR(null AND true, Value::kNullValue);
        TEST_EXPR(null AND false, false);
        TEST_EXPR(true AND null, Value::kNullValue);
        TEST_EXPR(false AND null, false);
        TEST_EXPR(null AND null, Value::kNullValue);
        TEST_EXPR(empty AND null AND empty, Value::kNullValue);

        TEST_EXPR(null OR true, true);
        TEST_EXPR(null OR false, Value::kNullValue);
        TEST_EXPR(true OR null, true);
        TEST_EXPR(false OR null, Value::kNullValue);
        TEST_EXPR(null OR null, Value::kNullValue);
        TEST_EXPR(empty OR null OR empty, Value::kNullValue);

        TEST_EXPR(null XOR true, Value::kNullValue);
        TEST_EXPR(null XOR false, Value::kNullValue);
        TEST_EXPR(true XOR null, Value::kNullValue);
        TEST_EXPR(false XOR null, Value::kNullValue);
        TEST_EXPR(null XOR null, Value::kNullValue);
        TEST_EXPR(empty XOR null XOR empty, Value::kNullValue);

        // test empty
        TEST_EXPR(empty, Value::kEmpty);
        TEST_EXPR(empty AND true, Value::kEmpty);
        TEST_EXPR(empty AND false, false);
        TEST_EXPR(true AND empty, Value::kEmpty);
        TEST_EXPR(false AND empty, false);
        TEST_EXPR(empty AND empty, Value::kEmpty);
        TEST_EXPR(empty AND null, Value::kNullValue);
        TEST_EXPR(null AND empty, Value::kNullValue);
        TEST_EXPR(empty AND true AND empty, Value::kEmpty);

        TEST_EXPR(empty OR true, true);
        TEST_EXPR(empty OR false, Value::kEmpty);
        TEST_EXPR(true OR empty, true);
        TEST_EXPR(false OR empty, Value::kEmpty);
        TEST_EXPR(empty OR empty, Value::kEmpty);
        TEST_EXPR(empty OR null, Value::kNullValue);
        TEST_EXPR(null OR empty, Value::kNullValue);
        TEST_EXPR(empty OR false OR empty, Value::kEmpty);

        TEST_EXPR(empty XOR true, Value::kEmpty);
        TEST_EXPR(empty XOR false, Value::kEmpty);
        TEST_EXPR(true XOR empty, Value::kEmpty);
        TEST_EXPR(false XOR empty, Value::kEmpty);
        TEST_EXPR(empty XOR empty, Value::kEmpty);
        TEST_EXPR(empty XOR null, Value::kNullValue);
        TEST_EXPR(null XOR empty, Value::kNullValue);
        TEST_EXPR(true XOR empty XOR false, Value::kEmpty);

        TEST_EXPR(empty OR false AND true AND null XOR empty, Value::kEmpty);
        TEST_EXPR(empty OR false AND true XOR empty OR true, true);
        TEST_EXPR((empty OR false) AND true XOR empty XOR null AND 2 / 0, Value::kNullValue);
        // empty OR false AND 2/0
        TEST_EXPR(empty OR false AND true XOR empty XOR null AND 2 / 0, Value::kEmpty);
        TEST_EXPR(empty AND true XOR empty XOR null AND 2 / 0, Value::kNullValue);
        TEST_EXPR(empty OR false AND true XOR empty OR null AND 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(empty OR false AND empty XOR empty OR null, Value::kNullValue);
    }
}

TEST_F(ExpressionTest, LiteralConstantsRelational) {
    {
        TEST_EXPR(true == 1.0, false);
        TEST_EXPR(true == 2.0, false);
        TEST_EXPR(true != 1.0, true);
        TEST_EXPR(true != 2.0, true);
        TEST_EXPR(true > 1.0, Value::kNullBadType);
        TEST_EXPR(true >= 1.0, Value::kNullBadType);
        TEST_EXPR(true < 1.0, Value::kNullBadType);
        TEST_EXPR(true <= 1.0, Value::kNullBadType);
        TEST_EXPR(false == 0.0, false);
        TEST_EXPR(false == 1.0, false);
        TEST_EXPR(false != 0.0, true);
        TEST_EXPR(false != 1.0, true);
        TEST_EXPR(false > 0.0, Value::kNullBadType);
        TEST_EXPR(false >= 0.0, Value::kNullBadType);
        TEST_EXPR(false < 0.0, Value::kNullBadType);
        TEST_EXPR(false <= 0.0, Value::kNullBadType);

        TEST_EXPR(true == 1, false);
        TEST_EXPR(true == 2, false);
        TEST_EXPR(true != 1, true);
        TEST_EXPR(true != 2, true);
        TEST_EXPR(true > 1, Value::kNullBadType);
        TEST_EXPR(true >= 1, Value::kNullBadType);
        TEST_EXPR(true < 1, Value::kNullBadType);
        TEST_EXPR(true <= 1, Value::kNullBadType);
        TEST_EXPR(false == 0, false);
        TEST_EXPR(false == 1, false);
        TEST_EXPR(false != 0, true);
        TEST_EXPR(false != 1, true);
        TEST_EXPR(false > 0, Value::kNullBadType);
        TEST_EXPR(false >= 0, Value::kNullBadType);
        TEST_EXPR(false < 0, Value::kNullBadType);
        TEST_EXPR(false <= 0, Value::kNullBadType);
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
        TEST_EXPR(empty == empty, true);
        TEST_EXPR(empty == null, Value::kNullValue);
        TEST_EXPR(empty != null, Value::kNullValue);
        TEST_EXPR(empty != 1, true);
        TEST_EXPR(empty != true, true);
        TEST_EXPR(empty > "1", Value::kEmpty);
        TEST_EXPR(empty < 1, Value::kEmpty);
        TEST_EXPR(empty >= 1.11, Value::kEmpty);

        TEST_EXPR(null != 1, Value::kNullValue);
        TEST_EXPR(null != true, Value::kNullValue);
        TEST_EXPR(null > "1", Value::kNullValue);
        TEST_EXPR(null < 1, Value::kNullValue);
        TEST_EXPR(null >= 1.11, Value::kNullValue);
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
        TEST_FUNCTION(substr, args_["substr"], "cdef");
        TEST_FUNCTION(left, args_["side"], "abcde");
        TEST_FUNCTION(right, args_["side"], "mnopq");
        TEST_FUNCTION(left, args_["neg_side"], Value::kNullValue);
        TEST_FUNCTION(right, args_["neg_side"], Value::kNullValue);

        TEST_FUNCTION(lpad, args_["pad"], "1231abcdefghijkl");
        TEST_FUNCTION(rpad, args_["pad"], "abcdefghijkl1231");
        TEST_FUNCTION(udf_is_in, args_["udf_is_in"], true);
    }
    {
        // hasSameEdgeInPath
        Path path;
        path.src.vid = "1";
        STEP("2", "edge", 0, 1);
        STEP("1", "edge", 0, -1);
        TEST_FUNCTION(hasSameEdgeInPath, {path}, true);
    }
    {
        // hasSameEdgeInPath
        Path path;
        path.src.vid = "0";
        Step step1, step2, step3;
        STEP("2", "edge", 0, 1);
        STEP("1", "edge", 0, -1);
        STEP("2", "edge", 0, 1);
        TEST_FUNCTION(hasSameEdgeInPath, {path}, true);
    }
    {
        // hasSameEdgeInPath
        Path path;
        path.src.vid = "0";
        Step step1, step2, step3;
        STEP("2", "edge", 0, 1);
        STEP("1", "edge", 0, 1);
        STEP("2", "edge", 0, 1);
        TEST_FUNCTION(hasSameEdgeInPath, {path}, false);
    }
    {
        // hasSameEdgeInPath
        Path path;
        path.src.vid = "0";
        Step step1, step2, step3;
        STEP("2", "edge", 0, 1);
        STEP("1", "edge", 0, -1);
        STEP("2", "edge", 1, 1);
        TEST_FUNCTION(hasSameEdgeInPath, {path}, false);
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
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        // e1.list_of_list == NULL
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new EdgePropertyExpression(new std::string("e1"), new std::string("list_of_list")),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
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
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        // NULL == NULL
        RelationalExpression expr(
                Expression::Kind::kRelEQ,
                new ConstantExpression(Value(NullType::NaN)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        // 1 != NULL
        RelationalExpression expr(
                Expression::Kind::kRelNE,
                new ConstantExpression(Value(1)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        // NULL != NULL
        RelationalExpression expr(
                Expression::Kind::kRelNE,
                new ConstantExpression(Value(NullType::NaN)),
                new ConstantExpression(Value(NullType::NaN)));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
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

TEST_F(ExpressionTest, RelIn) {
    {
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(1),
                new ConstantExpression(List({1, 2})));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(3),
                new ConstantExpression(List({1, 2})));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(Value::kNullValue),
                new ConstantExpression(List({1, 2})));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({1, 2});
        list.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(1),
                new ConstantExpression(list));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(1),
                new ConstantExpression(list));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(1),
                new ConstantExpression(list));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(list),
                new ConstantExpression(list));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list1 = List({3, 2});
        auto list2 = list1;
        list1.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(list1),
                new ConstantExpression(list2));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto list1 = List({3, 2});
        auto list2 = List({1});
        list2.emplace_back(list1);
        list2.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelIn,
                new ConstantExpression(list1),
                new ConstantExpression(list2));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
}

TEST_F(ExpressionTest, RelNotIn) {
    {
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(1),
                new ConstantExpression(List({1, 2})));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(3),
                new ConstantExpression(List({1, 2})));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(Value::kNullValue),
                new ConstantExpression(List({1, 2})));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({1, 2});
        list.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(1),
                new ConstantExpression(list));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(1),
                new ConstantExpression(list));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(1),
                new ConstantExpression(list));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list = List({3, 2});
        list.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(list),
                new ConstantExpression(list));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        auto list1 = List({3, 2});
        auto list2 = list1;
        list1.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(list1),
                new ConstantExpression(list2));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        auto list1 = List({3, 2});
        auto list2 = List({1});
        list2.emplace_back(list1);
        list2.emplace_back(Value::kNullValue);
        RelationalExpression expr(
                Expression::Kind::kRelNotIn,
                new ConstantExpression(list1),
                new ConstantExpression(list2));
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

TEST_F(ExpressionTest, IsNull) {
    {
        UnaryExpression expr(
                Expression::Kind::kIsNull,
                new ConstantExpression(Value::kNullValue));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNull,
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNull,
                new ConstantExpression(1.1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNull,
                new ConstantExpression(true));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNull,
                new ConstantExpression(Value::kEmpty));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
}


TEST_F(ExpressionTest, IsNotNull) {
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotNull,
                new ConstantExpression(Value::kNullValue));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotNull,
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotNull,
                new ConstantExpression(1.1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotNull,
                new ConstantExpression(true));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotNull,
                new ConstantExpression(Value::kEmpty));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
}


TEST_F(ExpressionTest, IsEmpty) {
    {
        UnaryExpression expr(
                Expression::Kind::kIsEmpty,
                new ConstantExpression(Value::kNullValue));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsEmpty,
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsEmpty,
                new ConstantExpression(1.1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsEmpty,
                new ConstantExpression(true));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsEmpty,
                new ConstantExpression(Value::kEmpty));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
}

TEST_F(ExpressionTest, IsNotEmpty) {
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotEmpty,
                new ConstantExpression(Value::kNullValue));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotEmpty,
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotEmpty,
                new ConstantExpression(1.1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotEmpty,
                new ConstantExpression(true));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        UnaryExpression expr(
                Expression::Kind::kIsNotEmpty,
                new ConstantExpression(Value::kEmpty));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
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
        EXPECT_EQ(ep.toString(), "{name:\"zhang\",hello:\"world\"}");
    }
    {
        ConstantExpression ep(Set({1, 2.3, "hello", true}));
        EXPECT_EQ(ep.toString(), "{\"hello\",2.3,true,1}");
    }
    {
        ConstantExpression ep(Date(1234));
        EXPECT_EQ(ep.toString(), "-32765-05-19");
    }
    {
        ConstantExpression ep(Edge("100", "102", 2, "like", 3, {{"likeness", 95}}));
        EXPECT_EQ(ep.toString(), "(\"100\")-[like(2)]->(\"102\")@3 likeness:95");
    }
    {
        ConstantExpression ep(Vertex("100", {Tag("player", {{"name", "jame"}})}));
        EXPECT_EQ(ep.toString(), "(\"100\") Tag: player, name:\"jame\"");
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

        UnaryExpression isNull(Expression::Kind::kIsNull, new ConstantExpression(2));
        EXPECT_EQ(isNull.toString(), "2 IS NULL");

        UnaryExpression isNotNull(Expression::Kind::kIsNotNull,
                                  new ConstantExpression(Value::kNullValue));
        EXPECT_EQ(isNotNull.toString(), "NULL IS NOT NULL");

        UnaryExpression isEmpty(Expression::Kind::kIsEmpty, new ConstantExpression(2));
        EXPECT_EQ(isEmpty.toString(), "2 IS EMPTY");

        UnaryExpression isNotEmpty(Expression::Kind::kIsNotEmpty,
                                  new ConstantExpression(Value::kEmpty));
        EXPECT_EQ(isNotEmpty.toString(), "__EMPTY__ IS NOT EMPTY");
    }
    {
        VariableExpression var(new std::string("name"));
        EXPECT_EQ(var.toString(), "$name");

        VersionedVariableExpression versionVar(new std::string("name"), new ConstantExpression(1));
        EXPECT_EQ(versionVar.toString(), "$name{1}");
    }
    {
        TEST_TOSTRING(2 + 2 - 3, "((2+2)-3)");
        TEST_TOSTRING(true OR true, "(true OR true)");
        TEST_TOSTRING(true AND false OR false, "((true AND false) OR false)");
        TEST_TOSTRING(true == 2, "(true==2)");
        TEST_TOSTRING(2 > 1 AND 3 > 2, "((2>1) AND (3>2))");
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
        DestPropertyExpression ep(new std::string("like"),
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
    ASSERT_EQ("[12345,\"Hello\",true]", expr->toString());
}

TEST_F(ExpressionTest, SetToString) {
    auto *elist = new ExpressionList();
    (*elist).add(new ConstantExpression(12345))
            .add(new ConstantExpression(12345))
            .add(new ConstantExpression("Hello"))
            .add(new ConstantExpression(true));
    auto expr = std::make_unique<SetExpression>(elist);
    ASSERT_EQ("{12345,12345,\"Hello\",true}", expr->toString());
}

TEST_F(ExpressionTest, AggregateToString) {
    auto* arg = new ConstantExpression("$-.age");
    auto* aggName = new std::string("COUNT");
    auto expr = std::make_unique<AggregateExpression>(aggName, arg, true);
    ASSERT_EQ("COUNT(distinct $-.age)", expr->toString());
}

TEST_F(ExpressionTest, MapTostring) {
    auto *items = new MapItemList();
    (*items).add(new std::string("key1"), new ConstantExpression(12345))
            .add(new std::string("key2"), new ConstantExpression(12345))
            .add(new std::string("key3"), new ConstantExpression("Hello"))
            .add(new std::string("key4"), new ConstantExpression(true));
    auto expr = std::make_unique<MapExpression>(items);
    auto expected = "{"
                        "key1:12345,"
                        "key2:12345,"
                        "key3:\"Hello\","
                        "key4:true"
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

TEST_F(ExpressionTest, DataSetSubscript) {
    {
        // dataset[]
        // [[0,1,2,3,4],[1,2,3,4,5],[2,3,4,5,6]] [0]
        DataSet ds;
        for (int32_t i = 0; i < 3; i++) {
            std::vector<Value> val;
            val.reserve(5);
            for (int32_t j = 0; j < 5; j++) {
                val.emplace_back(i + j);
            }
            ds.rows.emplace_back(List(std::move(val)));
        }
        auto *dataset = new ConstantExpression(ds);
        auto *rowIndex = new ConstantExpression(0);
        SubscriptExpression expr(dataset, rowIndex);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isList());
        ASSERT_EQ(Value(List({0, 1, 2, 3, 4})), value.getList());
    }
    {
        // dataset[][]
        // [[0,1,2,3,4],[1,2,3,4,5],[2,3,4,5,6]] [0][1]
        DataSet ds;
        for (int32_t i = 0; i < 3; i++) {
            std::vector<Value> val;
            val.reserve(5);
            for (int32_t j = 0; j < 5; j++) {
                val.emplace_back(i + j);
            }
            ds.rows.emplace_back(List(std::move(val)));
        }
        auto *dataset = new ConstantExpression(ds);
        auto *rowIndex = new ConstantExpression(0);
        auto *colIndex = new ConstantExpression(1);
        SubscriptExpression expr(new SubscriptExpression(dataset, rowIndex), colIndex);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
    {
        // dataset[]
        // [[0,1,2,3,4],[1,2,3,4,5],[2,3,4,5,6]] [-1]
        DataSet ds;
        for (int32_t i = 0; i < 3; i++) {
            std::vector<Value> val;
            val.reserve(5);
            for (int32_t j = 0; j < 5; j++) {
                val.emplace_back(i + j);
            }
            ds.rows.emplace_back(List(std::move(val)));
        }
        auto *dataset = new ConstantExpression(ds);
        auto *rowIndex = new ConstantExpression(-1);
        SubscriptExpression expr(dataset, rowIndex);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
    {
        // dataset[][]
        // [[0,1,2,3,4],[1,2,3,4,5],[2,3,4,5,6]] [0][5]
        DataSet ds;
        for (int32_t i = 0; i < 3; i++) {
            std::vector<Value> val;
            val.reserve(5);
            for (int32_t j = 0; j < 5; j++) {
                val.emplace_back(i + j);
            }
            ds.rows.emplace_back(List(std::move(val)));
        }
        auto *dataset = new ConstantExpression(ds);
        auto *rowIndex = new ConstantExpression(0);
        auto *colIndex = new ConstantExpression(5);
        SubscriptExpression expr(new SubscriptExpression(dataset, rowIndex), colIndex);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
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

TEST_F(ExpressionTest, VertexSubscript) {
    Vertex vertex;
    vertex.vid = "vid";
    vertex.tags.resize(2);
    vertex.tags[0].props = {
        {"Venus", "Mars"},
        {"Mull", "Kintyre"},
    };
    vertex.tags[1].props = {
        {"Bip", "Bop"},
        {"Tug", "War"},
        {"Venus", "RocksShow"},
    };
    {
        auto *left = new ConstantExpression(Value(vertex));
        auto *right = new ConstantExpression("Mull");
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Kintyre", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(vertex));
        auto *right = new LabelExpression("Bip");
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Bop", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(vertex));
        auto *right = new LabelExpression("Venus");
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Mars", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(vertex));
        auto *right = new LabelExpression("_vid");
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("vid", value.getStr());
    }
}

TEST_F(ExpressionTest, EdgeSubscript) {
    Edge edge;
    edge.name = "type";
    edge.src = "src";
    edge.dst = "dst";
    edge.ranking = 123;
    edge.props = {
        {"Magill", "Nancy"},
        {"Gideon", "Bible"},
        {"Rocky", "Raccoon"},
    };
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new ConstantExpression("Rocky");
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Raccoon", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new ConstantExpression(kType);
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("type", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new ConstantExpression(kSrc);
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("src", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new ConstantExpression(kDst);
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("dst", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new ConstantExpression(kRank);
        SubscriptExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(123, value.getInt());
    }
}

TEST_F(ExpressionTest, MapAttribute) {
    // {"key1":1, "key2":2, "key3":3}.key1
    {
        auto *items = new MapItemList();
        (*items).add(new std::string("key1"), new ConstantExpression(1))
                .add(new std::string("key2"), new ConstantExpression(2))
                .add(new std::string("key3"), new ConstantExpression(3));
        auto *map = new MapExpression(items);
        auto *key = new LabelExpression(new std::string("key1"));
        AttributeExpression expr(map, key);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
}

TEST_F(ExpressionTest, EdgeAttribute) {
    Edge edge;
    edge.name = "type";
    edge.src = "src";
    edge.dst = "dst";
    edge.ranking = 123;
    edge.props = {
        {"Magill", "Nancy"},
        {"Gideon", "Bible"},
        {"Rocky", "Raccoon"},
    };
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new LabelExpression(new std::string("Rocky"));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Raccoon", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new LabelExpression(new std::string(kType));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("type", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new LabelExpression(new std::string(kSrc));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("src", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new LabelExpression(new std::string(kDst));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("dst", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(edge));
        auto *right = new LabelExpression(new std::string(kRank));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(123, value.getInt());
    }
}

TEST_F(ExpressionTest, VertexAttribute) {
    Vertex vertex;
    vertex.vid = "vid";
    vertex.tags.resize(2);
    vertex.tags[0].props = {
        {"Venus", "Mars"},
        {"Mull", "Kintyre"},
    };
    vertex.tags[1].props = {
        {"Bip", "Bop"},
        {"Tug", "War"},
        {"Venus", "RocksShow"},
    };
    {
        auto *left = new ConstantExpression(Value(vertex));
        auto *right = new LabelExpression(new std::string("Mull"));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Kintyre", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(vertex));
        auto *right = new LabelExpression(new std::string("Bip"));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Bop", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(vertex));
        auto *right = new LabelExpression(new std::string("Venus"));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Mars", value.getStr());
    }
    {
        auto *left = new ConstantExpression(Value(vertex));
        auto *right = new LabelExpression(new std::string("_vid"));
        AttributeExpression expr(left, right);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("vid", value.getStr());
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
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
    }
    {
        TypeCastingExpression typeCast(Value::Type::BOOL, new ConstantExpression(0));
        auto eval = Expression::eval(&typeCast, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
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

TEST_F(ExpressionTest, RelationRegexMatch) {
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("abcd\xA3g1234efgh\x49ijkl"),
                new ConstantExpression("\\w{4}\xA3g12\\d*e\\w+\x49\\w+"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("Tony Parker"),
                new ConstantExpression("T.*er"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("010-12345"),
                new ConstantExpression("\\d{3}\\-\\d{3,8}"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("test_space_128"),
                new ConstantExpression("[a-zA-Z_][0-9a-zA-Z_]{0,19}"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("2001-09-01 08:00:00"),
                new ConstantExpression("\\d+\\-0\\d?\\-\\d+\\s\\d+:00:\\d+"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("jack138tom发."),
                new ConstantExpression("j\\w*\\d+\\w+\u53d1\\."));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("jack138tom\u53d1.34数数数"),
                new ConstantExpression("j\\w*\\d+\\w+发\\.34[\u4e00-\u9fa5]+"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("a good person"),
                new ConstantExpression("a\\s\\w+"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("Tony Parker"),
                new ConstantExpression("T\\w+\\s?\\P\\d+"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("010-12345"),
                new ConstantExpression("\\d?\\-\\d{3,8}"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("test_space_128牛"),
                new ConstantExpression("[a-zA-Z_][0-9a-zA-Z_]{0,19}"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        RelationalExpression expr(
                Expression::Kind::kRelREG,
                new ConstantExpression("2001-09-01 08:00:00"),
                new ConstantExpression("\\d+\\s\\d+:00:\\d+"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
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

TEST_F(ExpressionTest, RelationStartsWith) {
    {
        // "abc" starts with "a"
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("a"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" starts with "ab"
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("ab"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "1234"" starts with "12"
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression("1234"),
                new ConstantExpression("12"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "1234"" starts with "34"
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression("1234"),
                new ConstantExpression("34"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" starts with "bc"
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("bc"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" starts with "ac"
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("ac"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" starts with "AB"
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("AB"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" starts with 1
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression("abc1"),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 starts with 1
        RelationalExpression expr(
                Expression::Kind::kStartsWith,
                new ConstantExpression(1234),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
}

TEST_F(ExpressionTest, RelationNotStartsWith) {
    {
        // "abc" not starts with "a"
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("a"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" not starts with "ab"
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("ab"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "1234"" not starts with "12"
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression("1234"),
                new ConstantExpression("12"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "1234"" not starts with "34"
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression("1234"),
                new ConstantExpression("34"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not starts with "bc"
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("bc"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not starts with "ac"
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("ac"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not starts with "AB"
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("AB"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not starts with 1
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression("abc1"),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 not starts with 1
        RelationalExpression expr(
                Expression::Kind::kNotStartsWith,
                new ConstantExpression(1234),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
}

TEST_F(ExpressionTest, RelationEndsWith) {
    {
        // "abc" ends with "a"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("a"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" ends with "ab"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("ab"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "1234"" ends with "12"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("1234"),
                new ConstantExpression("12"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "1234"" ends with "34"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("1234"),
                new ConstantExpression("34"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" ends with "bc"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("bc"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" ends with "ac"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("ac"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" ends with "AB"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("AB"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" ends with "BC"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("BC"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" ends with 1
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("abc1"),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 ends with 1
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression(1234),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // "steve jobs" ends with "jobs"
        RelationalExpression expr(
                Expression::Kind::kEndsWith,
                new ConstantExpression("steve jobs"),
                new ConstantExpression("jobs"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
}

TEST_F(ExpressionTest, RelationNotEndsWith) {
    {
        // "abc" not ends with "a"
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("a"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not ends with "ab"
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("ab"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "1234"" not ends with "12"
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("1234"),
                new ConstantExpression("12"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "1234"" not ends with "34"
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("1234"),
                new ConstantExpression("34"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" not ends with "bc"
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("bc"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, false);
    }
    {
        // "abc" not ends with "ac"
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("ac"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not ends with "AB"
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("AB"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not ends with "BC"
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("abc"),
                new ConstantExpression("BC"));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::BOOL);
        EXPECT_EQ(eval, true);
    }
    {
        // "abc" not ends with 1
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
                new ConstantExpression("abc1"),
                new ConstantExpression(1));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::NULLVALUE);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        // 1234 not ends with 1
        RelationalExpression expr(
                Expression::Kind::kNotEndsWith,
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
        ASSERT_EQ("(\"abc\" CONTAINS \"a\")", expr.toString());
    }
}

TEST_F(ExpressionTest, NotContainsToString) {
    {
        // "abc" not contains "a"
        RelationalExpression expr(
                Expression::Kind::kNotContains,
                new ConstantExpression("abc"),
                new ConstantExpression("a"));
        ASSERT_EQ("(\"abc\" NOT CONTAINS \"a\")", expr.toString());
    }
}

TEST_F(ExpressionTest, LabelExprToString) {
    LabelExpression expr(new std::string("name"));
    ASSERT_EQ("name", expr.toString());
}

TEST_F(ExpressionTest, LabelEvaluate) {
    LabelExpression expr(new std::string("name"));
    auto value = Expression::eval(&expr, gExpCtxt);
    ASSERT_TRUE(value.isStr());
    ASSERT_EQ("name", value.getStr());
}

TEST_F(ExpressionTest, CaseExprToString) {
    {
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(24), new ConstantExpression(1));
        CaseExpression expr(cases);
        expr.setCondition(new ConstantExpression(23));
        ASSERT_EQ("CASE 23 WHEN 24 THEN 1 END", expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(24), new ConstantExpression(1));
        CaseExpression expr(cases);
        expr.setCondition(new ConstantExpression(23));
        expr.setDefault(new ConstantExpression(2));
        ASSERT_EQ("CASE 23 WHEN 24 THEN 1 ELSE 2 END", expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(1));
        cases->add(new ConstantExpression(true), new ConstantExpression(2));
        CaseExpression expr(cases);
        expr.setCondition(new RelationalExpression(Expression::Kind::kStartsWith,
                                                   new ConstantExpression("nebula"),
                                                   new ConstantExpression("nebu")));
        expr.setDefault(new ConstantExpression(3));
        ASSERT_EQ(
            "CASE (\"nebula\" STARTS WITH \"nebu\") WHEN false THEN 1 WHEN true THEN 2 ELSE 3 END",
            expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(7), new ConstantExpression(1));
        cases->add(new ConstantExpression(8), new ConstantExpression(2));
        cases->add(new ConstantExpression(8), new ConstantExpression("jack"));
        CaseExpression expr(cases);
        expr.setCondition(new ArithmeticExpression(
            Expression::Kind::kAdd, new ConstantExpression(3), new ConstantExpression(5)));
        expr.setDefault(new ConstantExpression(false));
        ASSERT_EQ("CASE (3+5) WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 8 THEN \"jack\" ELSE false END",
                  expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(18));
        CaseExpression expr(cases);
        ASSERT_EQ("CASE WHEN false THEN 18 END", expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(18));
        CaseExpression expr(cases);
        expr.setDefault(new ConstantExpression("ok"));
        ASSERT_EQ("CASE WHEN false THEN 18 ELSE \"ok\" END", expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(new RelationalExpression(Expression::Kind::kStartsWith,
                                            new ConstantExpression("nebula"),
                                            new ConstantExpression("nebu")),
                   new ConstantExpression("yes"));
        CaseExpression expr(cases);
        expr.setDefault(new ConstantExpression(false));
        ASSERT_EQ("CASE WHEN (\"nebula\" STARTS WITH \"nebu\") THEN \"yes\" ELSE false END",
                  expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelLT, new ConstantExpression(23), new ConstantExpression(17)),
            new ConstantExpression(1));
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelEQ, new ConstantExpression(37), new ConstantExpression(37)),
            new ConstantExpression(2));
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelNE, new ConstantExpression(45), new ConstantExpression(99)),
            new ConstantExpression(3));
        CaseExpression expr(cases);
        expr.setDefault(new ConstantExpression(4));
        ASSERT_EQ("CASE WHEN (23<17) THEN 1 WHEN (37==37) THEN 2 WHEN (45!=99) THEN 3 ELSE 4 END",
                  expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelLT, new ConstantExpression(23), new ConstantExpression(17)),
            new ConstantExpression(1));
        CaseExpression expr(cases, false);
        expr.setDefault(new ConstantExpression(2));
        ASSERT_EQ("((23<17) ? 1 : 2)", expr.toString());
    }
    {
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(1));
        CaseExpression expr(cases, false);
        expr.setDefault(new ConstantExpression("ok"));
        ASSERT_EQ("(false ? 1 : \"ok\")", expr.toString());
    }
}

TEST_F(ExpressionTest, CaseEvaluate) {
    {
        // CASE 23 WHEN 24 THEN 1 END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(24), new ConstantExpression(1));
        CaseExpression expr(cases);
        expr.setCondition(new ConstantExpression(23));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_EQ(value, Value::kNullValue);
    }
    {
        // CASE 23 WHEN 24 THEN 1 ELSE false END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(24), new ConstantExpression(1));
        CaseExpression expr(cases);
        expr.setCondition(new ConstantExpression(23));
        expr.setDefault(new ConstantExpression(false));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(value.getBool(), false);
    }
    {
        // CASE ("nebula" STARTS WITH "nebu") WHEN false THEN 1 WHEN true THEN 2 ELSE 3 END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(1));
        cases->add(new ConstantExpression(true), new ConstantExpression(2));
        CaseExpression expr(cases);
        expr.setCondition(new RelationalExpression(Expression::Kind::kStartsWith,
                                                   new ConstantExpression("nebula"),
                                                   new ConstantExpression("nebu")));
        expr.setDefault(new ConstantExpression(3));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(2, value.getInt());
    }
    {
        // CASE (3+5) WHEN 7 THEN 1 WHEN 8 THEN 2 WHEN 8 THEN "jack" ELSE "no" END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(7), new ConstantExpression(1));
        cases->add(new ConstantExpression(8), new ConstantExpression(2));
        cases->add(new ConstantExpression(8), new ConstantExpression("jack"));
        CaseExpression expr(cases);
        expr.setCondition(new ArithmeticExpression(
            Expression::Kind::kAdd, new ConstantExpression(3), new ConstantExpression(5)));
        expr.setDefault(new ConstantExpression("no"));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(2, value.getInt());
    }
    {
        // CASE WHEN false THEN 18 END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(18));
        CaseExpression expr(cases);
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_EQ(value, Value::kNullValue);
    }
    {
        // CASE WHEN false THEN 18 ELSE ok END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(18));
        CaseExpression expr(cases);
        expr.setDefault(new ConstantExpression("ok"));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("ok", value.getStr());
    }
    {
        // CASE WHEN "invalid when" THEN "no" ELSE 3 END
        auto *cases = new CaseList();
        cases->add(new ConstantExpression("invalid when"), new ConstantExpression("no"));
        CaseExpression expr(cases);
        expr.setDefault(new ConstantExpression(3));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_EQ(value, Value::kNullBadType);
    }
    {
        // CASE WHEN (23<17) THEN 1 WHEN (37==37) THEN 2 WHEN (45!=99) THEN 3 ELSE 4 END
        auto *cases = new CaseList();
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelLT, new ConstantExpression(23), new ConstantExpression(17)),
            new ConstantExpression(1));
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelEQ, new ConstantExpression(37), new ConstantExpression(37)),
            new ConstantExpression(2));
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelNE, new ConstantExpression(45), new ConstantExpression(99)),
            new ConstantExpression(3));
        CaseExpression expr(cases);
        expr.setDefault(new ConstantExpression(4));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(2, value.getInt());
    }
    {
        // ((23<17) ? 1 : 2)
        auto *cases = new CaseList();
        cases->add(
            new RelationalExpression(
                Expression::Kind::kRelLT, new ConstantExpression(23), new ConstantExpression(17)),
            new ConstantExpression(1));
        CaseExpression expr(cases, false);
        expr.setDefault(new ConstantExpression(2));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(2, value.getInt());
    }
    {
        // (false ? 1 : "ok")
        auto *cases = new CaseList();
        cases->add(new ConstantExpression(false), new ConstantExpression(1));
        CaseExpression expr(cases, false);
        expr.setDefault(new ConstantExpression("ok"));
        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("ok", value.getStr());
    }
}

TEST_F(ExpressionTest, ListComprehensionExprToString) {
    {
        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<ConstantExpression>(1));
        argList->addArgument(std::make_unique<ConstantExpression>(5));
        ListComprehensionExpression expr(
            new std::string("n"),
            new FunctionCallExpression(new std::string("range"), argList),
            new RelationalExpression(
                Expression::Kind::kRelGE,
                new LabelExpression(new std::string("n")),
                new ConstantExpression(2)));
        ASSERT_EQ("[n IN range(1,5) WHERE (n>=2)]", expr.toString());
    }
    {
        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<LabelExpression>(new std::string("p")));
        ListComprehensionExpression expr(
            new std::string("n"),
            new FunctionCallExpression(new std::string("nodes"), argList),
            nullptr,
            new ArithmeticExpression(
                Expression::Kind::kAdd,
                new LabelAttributeExpression(new LabelExpression(new std::string("n")),
                                             new ConstantExpression("age")),
                new ConstantExpression(10)));
        ASSERT_EQ("[n IN nodes(p) | (n.age+10)]", expr.toString());
    }
    {
        auto *listItems = new ExpressionList();
        (*listItems)
            .add(new ConstantExpression(0))
            .add(new ConstantExpression(1))
            .add(new ConstantExpression(2));
        ListComprehensionExpression expr(
            new std::string("n"),
            new ListExpression(listItems),
            new RelationalExpression(
                Expression::Kind::kRelGE,
                new LabelExpression(new std::string("n")),
                new ConstantExpression(2)),
            new ArithmeticExpression(
                Expression::Kind::kAdd,
                new LabelExpression(new std::string("n")),
                new ConstantExpression(10)));
        ASSERT_EQ("[n IN [0,1,2] WHERE (n>=2) | (n+10)]", expr.toString());
    }
}

TEST_F(ExpressionTest, ListComprehensionEvaluate) {
    {
        // [n IN [0, 1, 2, 4, 5] WHERE n >= 2 | n + 10]
        auto *listItems = new ExpressionList();
        (*listItems)
            .add(new ConstantExpression(0))
            .add(new ConstantExpression(1))
            .add(new ConstantExpression(2))
            .add(new ConstantExpression(4))
            .add(new ConstantExpression(5));
        ListComprehensionExpression expr(
            new std::string("n"),
            new ListExpression(listItems),
            new RelationalExpression(
                Expression::Kind::kRelGE,
                new VariableExpression(new std::string("n")),
                new ConstantExpression(2)),
            new ArithmeticExpression(
                Expression::Kind::kAdd,
                new VariableExpression(new std::string("n")),
                new ConstantExpression(10)));

        auto value = Expression::eval(&expr, gExpCtxt);
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

        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<VariableExpression>(new std::string("p")));
        ListComprehensionExpression expr(
            new std::string("n"),
            new FunctionCallExpression(new std::string("nodes"), argList),
            nullptr,
            new ArithmeticExpression(
                Expression::Kind::kAdd,
                new AttributeExpression(new VariableExpression(new std::string("n")),
                                        new ConstantExpression("age")),
                new ConstantExpression(5)));

        auto value = Expression::eval(&expr, gExpCtxt);
        List expected;
        expected.reserve(3);
        expected.emplace_back(23);
        expected.emplace_back(24);
        expected.emplace_back(25);
        ASSERT_TRUE(value.isList());
        ASSERT_EQ(expected, value.getList());
    }
}

TEST_F(ExpressionTest, PredicateExprToString) {
    {
        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<ConstantExpression>(1));
        argList->addArgument(std::make_unique<ConstantExpression>(5));
        PredicateExpression expr(
            new std::string("all"),
            new std::string("n"),
            new FunctionCallExpression(new std::string("range"), argList),
            new RelationalExpression(
                Expression::Kind::kRelGE,
                new LabelExpression(new std::string("n")),
                new ConstantExpression(2)));
        ASSERT_EQ("all(n IN range(1,5) WHERE (n>=2))", expr.toString());
    }
}

TEST_F(ExpressionTest, PredicateEvaluate) {
    {
        // all(n IN [0, 1, 2, 4, 5) WHERE n >= 2)
        auto *listItems = new ExpressionList();
        (*listItems)
            .add(new ConstantExpression(0))
            .add(new ConstantExpression(1))
            .add(new ConstantExpression(2))
            .add(new ConstantExpression(4))
            .add(new ConstantExpression(5));
        PredicateExpression expr(
            new std::string("all"),
            new std::string("n"),
            new ListExpression(listItems),
            new RelationalExpression(
                Expression::Kind::kRelGE,
                new VariableExpression(new std::string("n")),
                new ConstantExpression(2)));

        auto value = Expression::eval(&expr, gExpCtxt);
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

        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<VariableExpression>(new std::string("p")));
        PredicateExpression expr(
            new std::string("any"),
            new std::string("n"),
            new FunctionCallExpression(new std::string("nodes"), argList),
            new RelationalExpression(
                Expression::Kind::kRelGE,
                new AttributeExpression(new VariableExpression(new std::string("n")),
                                        new ConstantExpression("age")),
                new ConstantExpression(19)));

        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(true, value.getBool());
    }
    {
        // single(n IN [0, 1, 2, 4, 5) WHERE n == 2)
        auto *listItems = new ExpressionList();
        (*listItems)
            .add(new ConstantExpression(0))
            .add(new ConstantExpression(1))
            .add(new ConstantExpression(2))
            .add(new ConstantExpression(4))
            .add(new ConstantExpression(5));
        PredicateExpression expr(
            new std::string("single"),
            new std::string("n"),
            new ListExpression(listItems),
            new RelationalExpression(
                Expression::Kind::kRelEQ,
                new VariableExpression(new std::string("n")),
                new ConstantExpression(2)));

        auto value = Expression::eval(&expr, gExpCtxt);
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

        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<VariableExpression>(new std::string("p")));
        PredicateExpression expr(
            new std::string("none"),
            new std::string("n"),
            new FunctionCallExpression(new std::string("nodes"), argList),
            new RelationalExpression(
                Expression::Kind::kRelGE,
                new AttributeExpression(new VariableExpression(new std::string("n")),
                                        new ConstantExpression("age")),
                new ConstantExpression(19)));

        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_TRUE(value.isBool());
        ASSERT_EQ(false, value.getBool());
    }
    {
        // single(n IN null WHERE n > 1)
        PredicateExpression expr(
            new std::string("all"),
            new std::string("n"),
            new ConstantExpression(Value(NullType::__NULL__)),
            new RelationalExpression(
                Expression::Kind::kRelEQ,
                new VariableExpression(new std::string("n")),
                new ConstantExpression(1)));

        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_EQ(Value::kNullValue, value.getNull());
    }
}

TEST_F(ExpressionTest, ReduceExprToString) {
    {
        // reduce(totalNum = 2 * 10, n IN range(1, 5) | totalNum + n * 2)
        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<ConstantExpression>(1));
        argList->addArgument(std::make_unique<ConstantExpression>(5));
        ReduceExpression expr(
            new std::string("totalNum"),
            new ArithmeticExpression(
                Expression::Kind::kMultiply, new ConstantExpression(2), new ConstantExpression(10)),
            new std::string("n"),
            new FunctionCallExpression(new std::string("range"), argList),
            new ArithmeticExpression(
                Expression::Kind::kAdd,
                new LabelExpression(new std::string("totalNum")),
                new ArithmeticExpression(Expression::Kind::kMultiply,
                                         new LabelExpression(new std::string("n")),
                                         new ConstantExpression(2))));
        ASSERT_EQ("reduce(totalNum = (2*10), n IN range(1,5) | (totalNum+(n*2)))", expr.toString());
    }
}

TEST_F(ExpressionTest, ReduceEvaluate) {
    {
        // reduce(totalNum = 2 * 10, n IN range(1, 5) | totalNum + n * 2)
        ArgumentList *argList = new ArgumentList();
        argList->addArgument(std::make_unique<ConstantExpression>(1));
        argList->addArgument(std::make_unique<ConstantExpression>(5));
        ReduceExpression expr(
            new std::string("totalNum"),
            new ArithmeticExpression(
                Expression::Kind::kMultiply, new ConstantExpression(2), new ConstantExpression(10)),
            new std::string("n"),
            new FunctionCallExpression(new std::string("range"), argList),
            new ArithmeticExpression(
                Expression::Kind::kAdd,
                new VariableExpression(new std::string("totalNum")),
                new ArithmeticExpression(Expression::Kind::kMultiply,
                                         new VariableExpression(new std::string("n")),
                                         new ConstantExpression(2))));

        auto value = Expression::eval(&expr, gExpCtxt);
        ASSERT_EQ(Value::Type::INT, value.type());
        ASSERT_EQ(50, value.getInt());
    }
}

TEST_F(ExpressionTest, TestExprClone) {
    ConstantExpression expr(1);
    auto clone = expr.clone();
    ASSERT_EQ(*clone, expr);

    ArithmeticExpression aexpr(
        Expression::Kind::kAdd, new ConstantExpression(1), new ConstantExpression(1));
    auto aclone = aexpr.clone();
    ASSERT_EQ(*aclone, aexpr);

    AggregateExpression aggExpr(new std::string("COUNT"), new ConstantExpression("$-.*"), true);
    ASSERT_EQ(aggExpr, *aggExpr.clone());

    EdgeExpression edgeExpr;
    ASSERT_EQ(edgeExpr, *edgeExpr.clone());

    VertexExpression vertExpr;
    ASSERT_EQ(vertExpr, *vertExpr.clone());

    LabelExpression labelExpr(new std::string("label"));
    ASSERT_EQ(labelExpr, *labelExpr.clone());

    AttributeExpression attrExpr(new LabelExpression("label"), new LabelExpression("label"));
    ASSERT_EQ(attrExpr, *attrExpr.clone());

    LabelAttributeExpression labelAttrExpr(new LabelExpression(new std::string("label")),
                                           new ConstantExpression("prop"));
    ASSERT_EQ(labelAttrExpr, *labelAttrExpr.clone());

    TypeCastingExpression typeCastExpr(Value::Type::STRING, new ConstantExpression(100));
    ASSERT_EQ(typeCastExpr, *typeCastExpr.clone());

    FunctionCallExpression fnCallExpr(new std::string("count"), new ArgumentList);
    ASSERT_EQ(fnCallExpr, *fnCallExpr.clone());

    UUIDExpression uuidExpr(new std::string("hello"));
    ASSERT_EQ(uuidExpr, *uuidExpr.clone());

    SubscriptExpression subExpr(new VariableExpression(new std::string("var")),
                                new ConstantExpression(0));
    ASSERT_EQ(subExpr, *subExpr.clone());

    ListExpression listExpr;
    ASSERT_EQ(listExpr, *listExpr.clone());

    SetExpression setExpr;
    ASSERT_EQ(setExpr, *setExpr.clone());

    MapExpression mapExpr;
    ASSERT_EQ(mapExpr, *mapExpr.clone());

    EdgePropertyExpression edgePropExpr(new std::string("edge"), new std::string("prop"));
    ASSERT_EQ(edgePropExpr, *edgePropExpr.clone());

    TagPropertyExpression tagPropExpr(new std::string("tag"), new std::string("prop"));
    ASSERT_EQ(tagPropExpr, *tagPropExpr.clone());

    InputPropertyExpression inputPropExpr(new std::string("input"));
    ASSERT_EQ(inputPropExpr, *inputPropExpr.clone());

    VariablePropertyExpression varPropExpr(new std::string("var"), new std::string("prop"));
    ASSERT_EQ(varPropExpr, *varPropExpr.clone());

    SourcePropertyExpression srcPropExpr(new std::string("tag"), new std::string("prop"));
    ASSERT_EQ(srcPropExpr, *srcPropExpr.clone());

    DestPropertyExpression dstPropExpr(new std::string("tag"), new std::string("prop"));
    ASSERT_EQ(dstPropExpr, *dstPropExpr.clone());

    EdgeSrcIdExpression edgeSrcIdExpr(new std::string("edge"));
    ASSERT_EQ(edgeSrcIdExpr, *edgeSrcIdExpr.clone());

    EdgeTypeExpression edgeTypeExpr(new std::string("edge"));
    ASSERT_EQ(edgeTypeExpr, *edgeTypeExpr.clone());

    EdgeRankExpression edgeRankExpr(new std::string("edge"));
    ASSERT_EQ(edgeRankExpr, *edgeRankExpr.clone());

    EdgeDstIdExpression edgeDstIdExpr(new std::string("edge"));
    ASSERT_EQ(edgeDstIdExpr, *edgeDstIdExpr.clone());

    VariableExpression varExpr(new std::string("VARNAME"));
    auto varExprClone = varExpr.clone();
    ASSERT_EQ(varExpr, *varExprClone);

    VersionedVariableExpression verVarExpr(new std::string("VARNAME"), new ConstantExpression(0));
    ASSERT_EQ(*verVarExpr.clone(), verVarExpr);

    auto *cases = new CaseList();
    cases->add(new ConstantExpression(3), new ConstantExpression(9));
    CaseExpression caseExpr(cases);
    caseExpr.setCondition(new ConstantExpression(2));
    caseExpr.setDefault(new ConstantExpression(8));
    ASSERT_EQ(caseExpr, *caseExpr.clone());

    PathBuildExpression pathBuild;
    pathBuild.add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                            new std::string("path_src")))
        .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                            new std::string("path_edge1")))
        .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_v1")));
    ASSERT_EQ(pathBuild, *pathBuild.clone());

    ArgumentList *argList = new ArgumentList();
    argList->addArgument(std::make_unique<ConstantExpression>(1));
    argList->addArgument(std::make_unique<ConstantExpression>(5));
    ListComprehensionExpression lcExpr(
        new std::string("n"),
        new FunctionCallExpression(new std::string("range"), argList),
        new RelationalExpression(Expression::Kind::kRelGE,
                                 new LabelExpression(new std::string("n")),
                                 new ConstantExpression(2)));
    ASSERT_EQ(lcExpr, *lcExpr.clone());

    argList = new ArgumentList();
    argList->addArgument(std::make_unique<ConstantExpression>(1));
    argList->addArgument(std::make_unique<ConstantExpression>(5));
    PredicateExpression predExpr(
        new std::string("all"),
        new std::string("n"),
        new FunctionCallExpression(new std::string("range"), argList),
        new RelationalExpression(Expression::Kind::kRelGE,
                                 new LabelExpression(new std::string("n")),
                                 new ConstantExpression(2)));
    ASSERT_EQ(predExpr, *predExpr.clone());

    argList = new ArgumentList();
    argList->addArgument(std::make_unique<ConstantExpression>(1));
    argList->addArgument(std::make_unique<ConstantExpression>(5));
    ReduceExpression reduceExpr(
        new std::string("totalNum"),
        new ArithmeticExpression(
            Expression::Kind::kMultiply, new ConstantExpression(2), new ConstantExpression(10)),
        new std::string("n"),
        new FunctionCallExpression(new std::string("range"), argList),
        new ArithmeticExpression(Expression::Kind::kAdd,
                                 new LabelExpression(new std::string("totalNum")),
                                 new ArithmeticExpression(Expression::Kind::kMultiply,
                                                          new LabelExpression(new std::string("n")),
                                                          new ConstantExpression(2))));
    ASSERT_EQ(reduceExpr, *reduceExpr.clone());
}

TEST_F(ExpressionTest, PathBuild) {
    {
        PathBuildExpression expr;
        expr.add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_src")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_edge1")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_v1")));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::PATH);
        Path expected;
        expected.src = Vertex("1", {});
        expected.steps.emplace_back(Step(Vertex("2", {}), 1, "edge", 0, {}));
        EXPECT_EQ(eval.getPath(), expected);
    }
    {
        PathBuildExpression expr;
        expr.add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_src")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_edge1")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_v1")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_edge2")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_v2")));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::PATH);
        Path expected;
        expected.src = Vertex("1", {});
        expected.steps.emplace_back(Step(Vertex("2", {}), 1, "edge", 0, {}));
        expected.steps.emplace_back(Step(Vertex("3", {}), 1, "edge", 0, {}));
        EXPECT_EQ(eval.getPath(), expected);
    }
    {
        PathBuildExpression expr;
        expr.add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_src")));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::PATH);
        Path expected;
        expected.src = Vertex("1", {});
        EXPECT_EQ(eval.getPath(), expected);
    }
    {
        PathBuildExpression expr;

        expr.add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_src")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_edge1")));
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::PATH);
    }

    auto varPropExpr = [](const std::string &name) {
        auto var1 = std::make_unique<std::string>("var1");
        auto prop = std::make_unique<std::string>(name);
        return std::make_unique<VariablePropertyExpression>(var1.release(), prop.release());
    };

    {
        auto expr0 = std::make_unique<PathBuildExpression>();
        expr0->add(varPropExpr("path_src"));
        auto expr = std::make_unique<PathBuildExpression>();
        expr->add(std::move(expr0));
        expr->add(varPropExpr("path_edge1"));

        {
            // Test: Path + Edge
            auto result = Expression::eval(expr.get(), gExpCtxt);
            EXPECT_EQ(result.type(), Value::Type::PATH);
            const auto &path = result.getPath();
            EXPECT_EQ(path.steps.size(), 1);
            EXPECT_EQ(path.steps.back().dst.vid, "2");
        }

        auto expr1 = std::make_unique<PathBuildExpression>();
        expr1->add(varPropExpr("path_v1"));
        expr->add(std::move(expr1));

        {
            // Test: Path + Edge + Path
            auto result = Expression::eval(expr.get(), gExpCtxt);
            EXPECT_EQ(result.type(), Value::Type::PATH);
            const auto &path = result.getPath();
            EXPECT_EQ(path.steps.size(), 1);
            EXPECT_EQ(path.steps.back().dst.vid, "2");
        }

        expr->add(varPropExpr("path_edge2"));

        {
            // Test: Path + Edge + Path + Edge
            auto result = Expression::eval(expr.get(), gExpCtxt);
            EXPECT_EQ(result.type(), Value::Type::PATH);
            const auto &path = result.getPath();
            EXPECT_EQ(path.steps.size(), 2);
            EXPECT_EQ(path.steps.back().dst.vid, "3");
        }

        auto pathExpr2 = std::make_unique<PathBuildExpression>();
        pathExpr2->add(varPropExpr("path_v2"));
        pathExpr2->add(varPropExpr("path_edge3"));

        auto pathExpr3 = std::make_unique<PathBuildExpression>();
        pathExpr3->add(std::move(expr));
        pathExpr3->add(std::move(pathExpr2));

        {
            // Test: Path + Path
            auto result = Expression::eval(pathExpr3.get(), gExpCtxt);
            EXPECT_EQ(result.type(), Value::Type::PATH);
            const auto &path = result.getPath();
            EXPECT_EQ(path.steps.size(), 3);
            EXPECT_EQ(path.steps.back().dst.vid, "4");
        }
    }
}

TEST_F(ExpressionTest, PathBuildToString) {
    {
        PathBuildExpression expr;
        expr.add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_src")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_edge1")))
            .add(std::make_unique<VariablePropertyExpression>(new std::string("var1"),
                                                              new std::string("path_v1")));
        EXPECT_EQ(expr.toString(), "PathBuild[$var1.path_src,$var1.path_edge1,$var1.path_v1]");
    }
}

TEST_F(ExpressionTest, ColumnExpression) {
    {
        ColumnExpression expr(2);
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 3);
    }
    {
        ColumnExpression expr(0);
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 1);
    }
    {
        ColumnExpression expr(-1);
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 8);
    }
    {
        ColumnExpression expr(-3);
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval.type(), Value::Type::INT);
        EXPECT_EQ(eval, 6);
    }
    {
        ColumnExpression expr(8);
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        ColumnExpression expr(-8);
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        ColumnExpression expr(10);
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
    {
        ColumnExpression expr(-10);
        auto eval = Expression::eval(&expr, gExpCtxt);
        EXPECT_EQ(eval, Value::kNullBadType);
    }
}

TEST_F(ExpressionTest, AggregateExpression) {
    std::vector<std::pair<std::string, Value>> vals1_ =
        {{"a", 1},
         {"b", 4},
         {"c", 3},
         {"a", 3},
         {"c", 8},
         {"c", 5},
         {"c", 8}};

    std::vector<std::pair<std::string, Value>> vals2_ =
        {{"a", 1},
         {"b", 4},
         {"c", Value::kNullValue},
         {"c", 3},
         {"a", 3},
         {"a", Value::kEmpty},
         {"b", Value::kEmpty},
         {"c", Value::kEmpty},
         {"c", 8},
         {"a", Value::kNullValue},
         {"c", 5},
         {"b", Value::kNullValue},
         {"c", 8}};

    std::vector<std::pair<std::string, Value>> vals3_ =
        {{"a", 1},
         {"b", 4},
         {"c", 3},
         {"a", 3},
         {"c", 8},
         {"c", 5},
         {"c", 8}};

    std::vector<std::pair<std::string, Value>> vals4_ =
        {{"a", 1},
         {"b", 4},
         {"c", 3},
         {"c", Value::kNullValue},
         {"a", Value::kEmpty},
         {"b", Value::kEmpty},
         {"c", Value::kEmpty},
         {"a", Value::kNullValue},
         {"b", Value::kNullValue},
         {"a", 3},
         {"c", 8},
         {"c", 5},
         {"c", 8}};

    std::vector<std::pair<std::string, Value>> vals5_ =
        {{"c", Value::kNullValue},
         {"a", Value::kEmpty},
         {"b", Value::kEmpty},
         {"c", Value::kEmpty},
         {"a", Value::kNullValue},
         {"b", Value::kNullValue}};

    std::vector<std::pair<std::string, Value>> vals6_ =
        {{"a", true},
         {"b", false},
         {"c", true},
         {"a", false},
         {"c", true},
         {"c", false},
         {"c", true}};

    std::vector<std::pair<std::string, Value>> vals7_ =
           {{"a", 0},
            {"a", 1},
            {"a", 2},
            {"a", 3},
            {"a", 4},
            {"a", 5},
            {"a", 6},
            {"a", 7},
            {"b", 6},
            {"c", 7},
            {"c", 7},
            {"a", 8},
            {"c", 9},
            {"c", 9},
            {"a", 9}};

    std::vector<std::pair<std::string, Value>> vals8_ =
           {{"a", 0},
            {"a", 1},
            {"a", 2},
            {"c", Value::kEmpty},
            {"a", Value::kEmpty},
            {"a", 3},
            {"a", 4},
            {"a", 5},
            {"c", Value::kNullValue},
            {"a", 6},
            {"a", 7},
            {"b", Value::kEmpty},
            {"b", 6},
            {"a", Value::kNullValue},
            {"c", 7},
            {"c", 7},
            {"a", 8},
            {"c", 9},
            {"b", Value::kNullValue},
            {"c", 9},
            {"a", 9}};

    std::vector<std::pair<std::string, Value>> vals9_ =
        {{"a", true},
         {"b", true},
         {"c", false},
         {"a", false},
         {"c", false},
         {"c", false},
         {"c", true}};
    std::vector<std::pair<std::string, Value>> vals10_ =
        {{"a", "true"},
         {"b", "12"},
         {"c", "a"},
         {"a", "false"},
         {"c", "zxA"},
         {"c", "zxbC"},
         {"c", "Ca"}};


    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", 3},
                          {"b", 4},
                          {"c", 8}};
        TEST_AGG(, false, abs, vals1_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 3},
                          {"b", 4},
                          {"c", 5}};
        TEST_AGG(, true, abs, vals1_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", 2},
                          {"b", 1},
                          {"c", 4}};
        TEST_AGG(COUNT, false, abs, vals1_, expected1);
        TEST_AGG(COUNT, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 2},
                          {"b", 1},
                          {"c", 3}};
        TEST_AGG(COUNT, true, abs, vals1_, expected2);
        TEST_AGG(COUNT, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", 4},
                          {"b", 4},
                          {"c", 24}};
        TEST_AGG(SUM, false, abs, vals1_, expected1);
        TEST_AGG(SUM, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 4},
                          {"b", 4},
                          {"c", 16}};
        TEST_AGG(SUM, true, abs, vals1_, expected2);
        TEST_AGG(SUM, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", 2},
                          {"b", 4},
                          {"c", 6}};
        TEST_AGG(AVG, false, abs, vals1_, expected1);
        TEST_AGG(AVG, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 2},
                          {"b", 4},
                          {"c", 16.0/3}};
        TEST_AGG(AVG, true, abs, vals1_, expected2);
        TEST_AGG(AVG, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", 1},
                          {"b", 4},
                          {"c", 3}};
        TEST_AGG(MIN, false, abs, vals1_, expected1);
        TEST_AGG(MIN, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 1},
                          {"b", 4},
                          {"c", 3}};
        TEST_AGG(MIN, true, abs, vals1_, expected2);
        TEST_AGG(MIN, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", 3},
                          {"b", 4},
                          {"c", 8}};
        TEST_AGG(MAX, false, abs, vals1_, expected1);
        TEST_AGG(MAX, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 3},
                          {"b", 4},
                          {"c", 8}};
        TEST_AGG(MAX, true, abs, vals1_, expected2);
        TEST_AGG(MAX, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", Value(List({1, 3}))},
                          {"b", Value(List({4}))},
                          {"c", Value(List({3, 8, 5, 8}))}};
        TEST_AGG(COLLECT, false, abs, vals1_, expected1);
        TEST_AGG(COLLECT, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", List({1, 3})},
                          {"b", List({4})},
                          {"c", List({3, 8, 5})}};
        TEST_AGG(COLLECT, true, abs, vals1_, expected2);
        TEST_AGG(COLLECT, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"b", Set({4})},
                          {"a", Set({1, 3})},
                          {"c", Set({3, 8, 5})}};
        TEST_AGG(COLLECT_SET, false, abs, vals1_, expected1);
        TEST_AGG(COLLECT_SET, false, isConst, vals2_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"b", Set({4})},
                          {"a", Set({1, 3})},
                          {"c", Set({3, 8, 5})}};
        TEST_AGG(COLLECT_SET, true, abs, vals1_, expected2);
        TEST_AGG(COLLECT_SET, true, isConst, vals2_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", 2.8722813232690143},
                          {"b", 0.0},
                          {"c", 0.9999999999999999}};
        TEST_AGG(STD, false, abs, vals7_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 2.8722813232690143},
                          {"b", 0.0},
                          {"c", 1.0}};
        TEST_AGG(STD, true, abs, vals7_, expected2);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", 1},
                          {"b", 4},
                          {"c", 0}};
        TEST_AGG(BIT_AND, false, abs, vals3_, expected1);
        TEST_AGG(BIT_AND, false, isConst, vals4_, expected1);

        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 1},
                          {"b", 4},
                          {"c", 0}};
        TEST_AGG(BIT_AND, true, abs, vals3_, expected2);
        TEST_AGG(BIT_AND, true, isConst, vals4_, expected2);

        const std::unordered_map<std::string, Value>
             expected3 = {{"a", 3},
                          {"b", 4},
                          {"c", 15}};
        TEST_AGG(BIT_OR, false, abs, vals3_, expected3);
        TEST_AGG(BIT_OR, false, isConst, vals4_, expected3);

        const std::unordered_map<std::string, Value>
             expected4 = {{"a", 3},
                          {"b", 4},
                          {"c", 15}};
        TEST_AGG(BIT_OR, true, abs, vals3_, expected4);
        TEST_AGG(BIT_OR, true, isConst, vals4_, expected4);

        const std::unordered_map<std::string, Value>
             expected5 = {{"a", 2},
                          {"b", 4},
                          {"c", 6}};
        TEST_AGG(BIT_XOR, false, abs, vals3_, expected5);
        TEST_AGG(BIT_XOR, false, isConst, vals4_, expected5);

        const std::unordered_map<std::string, Value>
             expected6 = {{"a", 2},
                          {"b", 4},
                          {"c", 14}};
        TEST_AGG(BIT_XOR, true, abs, vals3_, expected6);
        TEST_AGG(BIT_XOR, true, isConst, vals4_, expected6);
    }
    {
        const std::unordered_map<std::string, Value>
             expected1 = {{"a", Value::kNullValue},
                          {"b", Value::kNullValue},
                          {"c", Value::kNullValue}};
        const std::unordered_map<std::string, Value>
             expected2 = {{"a", 0},
                          {"b", 0},
                          {"c", 0}};
        const std::unordered_map<std::string, Value>
             expected3 = {{"a", Value(List())},
                          {"b", Value(List())},
                          {"c", Value(List())}};
        const std::unordered_map<std::string, Value>
             expected4 = {{"a", Value(Set())},
                          {"b", Value(Set())},
                          {"c", Value(Set())}};
        const std::unordered_map<std::string, Value>
             expected5 = {{"a", Value::kNullBadType},
                          {"b", Value::kNullBadType},
                          {"c", Value::kNullBadType}};
        const std::unordered_map<std::string, Value>
             expected6 = {{"a", false},
                          {"b", true},
                          {"c", false}};
        const std::unordered_map<std::string, Value>
             expected7 = {{"a", true},
                          {"b", true},
                          {"c", true}};

        TEST_AGG(COUNT, false, isConst, vals5_, expected2);
        TEST_AGG(COUNT, true, isConst, vals5_, expected2);
        TEST_AGG(COUNT, false, abs, vals9_, expected5);
        TEST_AGG(COUNT, true, abs, vals9_, expected5);
        TEST_AGG(SUM, false, isConst, vals5_, expected1);
        TEST_AGG(SUM, true, isConst, vals5_, expected1);
        TEST_AGG(SUM, false, isConst, vals9_, expected5);
        TEST_AGG(SUM, true, isConst, vals9_, expected5);
        TEST_AGG(AVG, false, isConst, vals5_, expected1);
        TEST_AGG(AVG, true, isConst, vals5_, expected1);
        TEST_AGG(AVG, false, isConst, vals9_, expected5);
        TEST_AGG(AVG, true, isConst, vals9_, expected5);
        TEST_AGG(MAX, false, isConst, vals5_, expected1);
        TEST_AGG(MAX, true, isConst, vals5_, expected1);
        TEST_AGG(MAX, false, isConst, vals9_, expected7);
        TEST_AGG(MAX, true, isConst, vals9_, expected7);
        TEST_AGG(MIN, false, isConst, vals5_, expected1);
        TEST_AGG(MIN, true, isConst, vals5_, expected1);
        TEST_AGG(MIN, false, isConst, vals9_, expected6);
        TEST_AGG(MIN, true, isConst, vals9_, expected6);
        TEST_AGG(STD, false, isConst, vals5_, expected1);
        TEST_AGG(STD, true, isConst, vals5_, expected1);
        TEST_AGG(STD, false, isConst, vals9_, expected5);
        TEST_AGG(STD, true, isConst, vals9_, expected5);
        TEST_AGG(BIT_AND, false, isConst, vals5_, expected1);
        TEST_AGG(BIT_AND, true, isConst, vals5_, expected1);
        TEST_AGG(BIT_AND, false, isConst, vals6_, expected5);
        TEST_AGG(BIT_AND, true, isConst, vals6_, expected5);
        TEST_AGG(BIT_AND, false, isConst, vals9_, expected5);
        TEST_AGG(BIT_AND, true, isConst, vals9_, expected5);
        TEST_AGG(BIT_OR, false, isConst, vals5_, expected1);
        TEST_AGG(BIT_OR, true, isConst, vals5_, expected1);
        TEST_AGG(BIT_OR, false, isConst, vals6_, expected5);
        TEST_AGG(BIT_OR, true, isConst, vals6_, expected5);
        TEST_AGG(BIT_OR, false, isConst, vals9_, expected5);
        TEST_AGG(BIT_OR, true, isConst, vals9_, expected5);
        TEST_AGG(BIT_XOR, false, isConst, vals5_, expected1);
        TEST_AGG(BIT_XOR, true, isConst, vals5_, expected1);
        TEST_AGG(BIT_XOR, false, isConst, vals6_, expected5);
        TEST_AGG(BIT_XOR, true, isConst, vals6_, expected5);
        TEST_AGG(BIT_XOR, false, isConst, vals9_, expected5);
        TEST_AGG(BIT_XOR, true, isConst, vals9_, expected5);
        TEST_AGG(COLLECT, false, isConst, vals5_, expected3);
        TEST_AGG(COLLECT, true, isConst, vals5_, expected3);
        TEST_AGG(COLLECT_SET, false, isConst, vals5_, expected4);
        TEST_AGG(COLLECT_SET, true, isConst, vals5_, expected4);
    }
}
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
