/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_EXPRESSION_TEST_TESTBASE_H_
#define COMMON_EXPRESSION_TEST_TESTBASE_H_

#include <gtest/gtest.h>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/base/ObjectPool.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Path.h"
#include "common/datatypes/Set.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Vertex.h"
#include "common/expression/AggregateExpression.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/AttributeExpression.h"
#include "common/expression/CaseExpression.h"
#include "common/expression/ColumnExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/EdgeExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/ListComprehensionExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PathBuildExpression.h"
#include "common/expression/PredicateExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/ReduceExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/VertexExpression.h"
#include "common/expression/test/ExpressionContextMock.h"

nebula::ExpressionContextMock gExpCtxt;
nebula::ObjectPool pool;
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

protected:
    static std::unordered_map<std::string, Value> boolen_;
    // static std::unordered_map<std::string, Expression::Kind> op_;
    static std::unordered_map<
        std::string,
        std::function<Expression *(ObjectPool *pool, Expression *lhs, Expression *rhs)>>
        op_;

protected:
    Expression *ExpressionCalu(const std::vector<std::string> &expr) {
        std::vector<std::string> relationOp = {">", ">=", "<", "<=", "==", "!="};
        std::vector<std::string> logicalOp = {"AND", "OR", "XOR"};
        std::vector<std::string> arithmeticOp = {"+", "-", "*", "/", "%"};

        std::vector<std::string> symbol = InfixToSuffix(expr);
        if (symbol.size() == 1) {
            // TEST_EXPR(true, true)
            if (boolen_.find(symbol.front()) != boolen_.end()) {
                return ConstantExpression::make(&pool, boolen_[symbol.front()]);
            } else if (symbol.front().find('.') != std::string::npos) {
                // TEST_EXPR(123.0, 123.0)
                return ConstantExpression::make(&pool, ::atof(symbol.front().c_str()));
            }
            // TEST_EXPR(123, 123)
            return ConstantExpression::make(&pool, ::atoi(symbol.front().c_str()));
        }

        // calu suffix expression
        std::stack<Expression *> value;
        for (const auto &str : symbol) {
            if (op_.find(str) == op_.end()) {
                Expression *ep = nullptr;
                if (boolen_.find(str) != boolen_.end()) {
                    ep = ConstantExpression::make(&pool, boolen_[str.c_str()]);
                } else if (str.find('.') != std::string::npos) {
                    ep = ConstantExpression::make(&pool, ::atof(str.c_str()));
                } else {
                    ep = ConstantExpression::make(&pool, ::atoi(str.c_str()));
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
                    result = op_[str](&pool, lhs, rhs);
                } else if (std::find(relationOp.begin(), relationOp.end(), str) !=
                           relationOp.end()) {
                    result = op_[str](&pool, lhs, rhs);
                } else if (std::find(logicalOp.begin(), logicalOp.end(), str) != logicalOp.end()) {
                    result = op_[str](&pool, lhs, rhs);
                } else {
                    return ConstantExpression::make(&pool, NullType::UNKNOWN_PROP);
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
    }

    void testToString(const std::string &exprSymbol, const char *expected) {
        std::string expr(exprSymbol);
        InsertSpace(expr);
        std::vector<std::string> splitString;
        boost::split(splitString, expr, boost::is_any_of(" \t"));
        Expression *ep = ExpressionCalu(splitString);
        EXPECT_EQ(ep->toString(), expected);
    }

    void testFunction(const char *name, const std::vector<Value> &args, const Value &expected) {
        ArgumentList *argList = ArgumentList::make(&pool);
        for (const auto &i : args) {
            argList->addArgument(ConstantExpression::make(&pool, i));
        }
        auto functionCall = FunctionCallExpression::make(&pool, name, argList);
        auto eval = Expression::eval(functionCall, gExpCtxt);
        // EXPECT_EQ(eval.type(), expected.type());
        EXPECT_EQ(eval, expected);
    }
};
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

// Functions used to construct corresponding expressions
using expressionGen =
    std::function<Expression *(ObjectPool *pool, Expression *lhs, Expression *rhs)>;

expressionGen makeAddExpr{&ArithmeticExpression::makeAdd};
expressionGen makeMinusExpr{&ArithmeticExpression::makeMinus};
expressionGen makeMultiplyExpr{&ArithmeticExpression::makeMultiply};
expressionGen makeDivisionExpr{&ArithmeticExpression::makeDivision};
expressionGen makeModExpr{&ArithmeticExpression::makeMod};
expressionGen makeOrExpr{
    static_cast<LogicalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &LogicalExpression::makeOr)};
expressionGen makeAndExpr{
    static_cast<LogicalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &LogicalExpression::makeAnd)};
expressionGen makeXorExpr{
    static_cast<LogicalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &LogicalExpression::makeXor)};
expressionGen makeRelGT{
    static_cast<RelationalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &RelationalExpression::makeGT)};
expressionGen makeRelLT{
    static_cast<RelationalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &RelationalExpression::makeLT)};
expressionGen makeRelGE{
    static_cast<RelationalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &RelationalExpression::makeGE)};
expressionGen makeRelLE{
    static_cast<RelationalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &RelationalExpression::makeLE)};
expressionGen makeRelEQ{
    static_cast<RelationalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &RelationalExpression::makeEQ)};
expressionGen makeRelNE{
    static_cast<RelationalExpression *(*)(ObjectPool *pool, Expression *lhs, Expression *rhs)>(
        &RelationalExpression::makeNE)};

std::unordered_map<std::string, expressionGen> ExpressionTest::op_ = {{"+", makeAddExpr},
                                                                      {"-", makeMinusExpr},
                                                                      {"*", makeMultiplyExpr},
                                                                      {"/", makeDivisionExpr},
                                                                      {"%", makeModExpr},
                                                                      {"OR", makeOrExpr},
                                                                      {"AND", makeAndExpr},
                                                                      {"XOR", makeXorExpr},
                                                                      {">", makeRelGT},
                                                                      {"<", makeRelLT},
                                                                      {">=", makeRelGE},
                                                                      {"<=", makeRelLE},
                                                                      {"==", makeRelEQ},
                                                                      {"!=", makeRelNE}};

std::unordered_map<std::string, Value> ExpressionTest::boolen_ = {
    {"true", Value(true)},
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

}   // namespace nebula
#endif   // COMMON_EXPRESSION_TEST_TESTBASE_H_
