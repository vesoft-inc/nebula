/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartRe...
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartRe...

#include <string>  // for string

#include "common/datatypes/List.h"                         // for List
#include "common/datatypes/Map.h"                          // for Map
#include "common/datatypes/Set.h"                          // for Set
#include "common/datatypes/Value.h"                        // for Value, hash
#include "common/expression/ConstantExpression.h"          // for ConstantEx...
#include "common/expression/ContainerExpression.h"         // for Expression...
#include "common/expression/Expression.h"                  // for Expression
#include "common/expression/test/ExpressionContextMock.h"  // for Expression...
#include "common/expression/test/TestBase.h"               // for pool, Expr...

namespace nebula {

class ConstantExpressionTest : public ExpressionTest {};

TEST_F(ExpressionTest, ListToString) {
  auto elist = ExpressionList::make(&pool);
  (*elist)
      .add(ConstantExpression::make(&pool, 12345))
      .add(ConstantExpression::make(&pool, "Hello"))
      .add(ConstantExpression::make(&pool, true));
  auto expr = ListExpression::make(&pool, elist);
  ASSERT_EQ("[12345,\"Hello\",true]", expr->toString());
}

TEST_F(ExpressionTest, SetToString) {
  auto elist = ExpressionList::make(&pool);
  (*elist)
      .add(ConstantExpression::make(&pool, 12345))
      .add(ConstantExpression::make(&pool, 12345))
      .add(ConstantExpression::make(&pool, "Hello"))
      .add(ConstantExpression::make(&pool, true));
  auto expr = SetExpression::make(&pool, elist);
  ASSERT_EQ("{12345,12345,\"Hello\",true}", expr->toString());
}

TEST_F(ExpressionTest, MapTostring) {
  auto *items = MapItemList::make(&pool);
  (*items)
      .add("key1", ConstantExpression::make(&pool, 12345))
      .add("key2", ConstantExpression::make(&pool, 12345))
      .add("key3", ConstantExpression::make(&pool, "Hello"))
      .add("key4", ConstantExpression::make(&pool, true));
  auto expr = MapExpression::make(&pool, items);
  auto expected =
      "{"
      "key1:12345,"
      "key2:12345,"
      "key3:\"Hello\","
      "key4:true"
      "}";
  ASSERT_EQ(expected, expr->toString());
}

TEST_F(ExpressionTest, ListEvaluate) {
  auto elist = ExpressionList::make(&pool);
  (*elist)
      .add(ConstantExpression::make(&pool, 12345))
      .add(ConstantExpression::make(&pool, "Hello"))
      .add(ConstantExpression::make(&pool, true));
  auto expr = ListExpression::make(&pool, elist);
  auto expected = Value(List({12345, "Hello", true}));
  auto value = Expression::eval(expr, gExpCtxt);
  ASSERT_EQ(expected, value);
}

TEST_F(ExpressionTest, SetEvaluate) {
  auto elist = ExpressionList::make(&pool);
  (*elist)
      .add(ConstantExpression::make(&pool, 12345))
      .add(ConstantExpression::make(&pool, 12345))
      .add(ConstantExpression::make(&pool, "Hello"))
      .add(ConstantExpression::make(&pool, true));
  auto expr = SetExpression::make(&pool, elist);
  auto expected = Value(Set({12345, "Hello", true}));
  auto value = Expression::eval(expr, gExpCtxt);
  ASSERT_EQ(expected, value);
}

TEST_F(ExpressionTest, MapEvaluate) {
  {
    auto *items = MapItemList::make(&pool);
    (*items)
        .add("key1", ConstantExpression::make(&pool, 12345))
        .add("key2", ConstantExpression::make(&pool, 12345))
        .add("key3", ConstantExpression::make(&pool, "Hello"))
        .add("key4", ConstantExpression::make(&pool, true));
    auto expr = MapExpression::make(&pool, items);
    auto expected =
        Value(Map({{"key1", 12345}, {"key2", 12345}, {"key3", "Hello"}, {"key4", true}}));
    auto value = Expression::eval(expr, gExpCtxt);
    ASSERT_EQ(expected, value);
  }
  {
    auto *items = MapItemList::make(&pool);
    (*items)
        .add("key1", ConstantExpression::make(&pool, 12345))
        .add("key2", ConstantExpression::make(&pool, 12345))
        .add("key3", ConstantExpression::make(&pool, "Hello"))
        .add("key4", ConstantExpression::make(&pool, false))
        .add("key4", ConstantExpression::make(&pool, true));
    auto expr = MapExpression::make(&pool, items);
    auto expected =
        Value(Map({{"key1", 12345}, {"key2", 12345}, {"key3", "Hello"}, {"key4", false}}));
    auto value = Expression::eval(expr, gExpCtxt);
    ASSERT_EQ(expected, value);
  }
}
}  // namespace nebula
