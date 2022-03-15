/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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
#include "parser/GQLParser.h"

namespace nebula {

extern ExpressionContextMock gExpCtxt;
extern ObjectPool pool;
extern std::unordered_map<std::string, std::vector<Value>> args_;

class ExpressionTest : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

 protected:
  void testExpr(const std::string &exprSymbol, Value expected) {
    std::string query = "RETURN " + exprSymbol;
    nebula::graph::QueryContext queryCtxt;
    nebula::GQLParser gParser(&queryCtxt);
    auto result = gParser.parse(query);
    ASSERT_EQ(result.ok(), true);
    auto *sequentialSentences = static_cast<SequentialSentences *>(result.value().get());
    ASSERT_NE(sequentialSentences, nullptr);
    auto sentences = sequentialSentences->sentences();
    ASSERT_GT(sentences.size(), 0);
    auto *yieldSentence = static_cast<YieldSentence *>(sentences[0]);
    ASSERT_NE(yieldSentence, nullptr);
    ASSERT_NE(yieldSentence->yield(), nullptr);
    ASSERT_NE(yieldSentence->yield()->yields(), nullptr);
    ASSERT_NE(yieldSentence->yield()->yields()->back(), nullptr);
    Expression *ep = yieldSentence->yield()->yields()->back()->expr();
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), expected.type()) << "type check failed: " << ep->toString();
    EXPECT_EQ(eval, expected) << "check failed: " << ep->toString();
  }

  void testToString(const std::string &exprSymbol, const char *expected) {
    std::string query = "RETURN " + exprSymbol;
    nebula::graph::QueryContext queryCtxt;
    nebula::GQLParser gParser(&queryCtxt);
    auto result = gParser.parse(query);
    ASSERT_EQ(result.ok(), true);
    auto *sequentialSentences = static_cast<SequentialSentences *>(result.value().get());
    ASSERT_NE(sequentialSentences, nullptr);
    auto sentences = sequentialSentences->sentences();
    ASSERT_GT(sentences.size(), 0);
    auto *yieldSentence = static_cast<YieldSentence *>(sentences[0]);
    ASSERT_NE(yieldSentence, nullptr);
    ASSERT_NE(yieldSentence->yield(), nullptr);
    ASSERT_NE(yieldSentence->yield()->yields(), nullptr);
    ASSERT_NE(yieldSentence->yield()->yields()->back(), nullptr);
    Expression *ep = yieldSentence->yield()->yields()->back()->expr();
    ASSERT_NE(ep, nullptr);
    EXPECT_EQ(ep->toString(), expected);
  }

  void testFunction(const char *name, const std::vector<Value> &args, const Value &expected) {
    std::string query = "RETURN " + std::string(name) + "(";
    for (const auto &i : args) {
      query += i.toString() + ",";
    }
    if (query.back() == ',') {
      query.pop_back();
    }
    query += ")";
    nebula::graph::QueryContext queryCtxt;
    nebula::GQLParser gParser(&queryCtxt);
    auto result = gParser.parse(query);
    ASSERT_EQ(result.ok(), true);
    auto *sequentialSentences = static_cast<SequentialSentences *>(result.value().get());
    ASSERT_NE(sequentialSentences, nullptr);
    auto sentences = sequentialSentences->sentences();
    ASSERT_GT(sentences.size(), 0);
    auto *yieldSentence = static_cast<YieldSentence *>(sentences[0]);
    ASSERT_NE(yieldSentence, nullptr);
    ASSERT_NE(yieldSentence->yield(), nullptr);
    ASSERT_NE(yieldSentence->yield()->yields(), nullptr);
    ASSERT_NE(yieldSentence->yield()->yields()->back(), nullptr);
    auto eval = Expression::eval(yieldSentence->yield()->yields()->back()->expr(), gExpCtxt);
    // EXPECT_EQ(eval.type(), expected.type());
    EXPECT_EQ(eval, expected);
  }

  void testPathFunction(const char *name, const std::vector<Value> &args, const Value &expected) {
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
#define TEST_EXPR(expr, expected) \
  do {                            \
    testExpr(#expr, expected);    \
  } while (0)

#define TEST_FUNCTION(expr, args, expected) \
  do {                                      \
    testFunction(#expr, args, expected);    \
  } while (0)

#define TEST_PATH_FUNCTION(expr, args, expected) \
  do {                                           \
    testPathFunction(#expr, args, expected);     \
  } while (0)

#define TEST_TOSTRING(expr, expected) \
  do {                                \
    testToString(#expr, expected);    \
  } while (0)

#define STEP(DST, NAME, RANKING, TYPE)        \
  do {                                        \
    Step step;                                \
    step.dst.vid = DST;                       \
    step.name = NAME;                         \
    step.ranking = RANKING;                   \
    step.type = TYPE;                         \
    path.steps.emplace_back(std::move(step)); \
  } while (0)

}  // namespace nebula

#endif  // COMMON_EXPRESSION_TEST_TESTBASE_H_
