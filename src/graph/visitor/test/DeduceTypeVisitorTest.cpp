/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>  // for TestPartResult
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartResult

#include <string>       // for string, basic_string
#include <type_traits>  // for remove_reference<...
#include <utility>      // for move

#include "common/base/Status.h"                     // for operator<<
#include "common/datatypes/Edge.h"                  // for Edge
#include "common/datatypes/Map.h"                   // for Map
#include "common/datatypes/Value.h"                 // for Value, Value::Type
#include "common/datatypes/Vertex.h"                // for Tag, Vertex
#include "common/expression/AttributeExpression.h"  // for AttributeExpression
#include "common/expression/ConstantExpression.h"   // for ConstantExpression
#include "common/expression/ContainerExpression.h"  // for ExpressionList
#include "common/expression/SubscriptExpression.h"  // for SubscriptExpression
#include "graph/context/Symbols.h"                  // for ColsDef
#include "graph/visitor/DeduceTypeVisitor.h"        // for DeduceTypeVisitor
#include "graph/visitor/test/VisitorTestBase.h"     // for VisitorTestBase

namespace nebula {
namespace graph {
class DeduceTypeVisitorTest : public VisitorTestBase {
 protected:
  static ColsDef emptyInput_;
};

ColsDef DeduceTypeVisitorTest::emptyInput_;

TEST_F(DeduceTypeVisitorTest, SubscriptExpr) {
  {
    auto* items = ExpressionList::make(pool);
    items->add(ConstantExpression::make(pool, 1));
    auto expr = SubscriptExpression::make(
        pool, ListExpression::make(pool, items), ConstantExpression::make(pool, 1));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = SubscriptExpression::make(
        pool, ConstantExpression::make(pool, Value::kNullValue), ConstantExpression::make(pool, 1));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok()) << std::move(visitor).status();
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = SubscriptExpression::make(
        pool, ConstantExpression::make(pool, Value::kEmpty), ConstantExpression::make(pool, 1));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = SubscriptExpression::make(pool,
                                          ConstantExpression::make(pool, Value::kNullValue),
                                          ConstantExpression::make(pool, Value::kNullValue));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = SubscriptExpression::make(
        pool,
        ConstantExpression::make(pool, Value(Map({{"k", "v"}, {"k1", "v1"}}))),
        ConstantExpression::make(pool, "test"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = SubscriptExpression::make(pool,
                                          ConstantExpression::make(pool, Value::kEmpty),
                                          ConstantExpression::make(pool, "test"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = SubscriptExpression::make(pool,
                                          ConstantExpression::make(pool, Value::kNullValue),
                                          ConstantExpression::make(pool, "test"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok()) << std::move(visitor).status();
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }

  // exceptions
  {
    auto expr = SubscriptExpression::make(
        pool, ConstantExpression::make(pool, "test"), ConstantExpression::make(pool, 1));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_FALSE(visitor.ok());
  }
  {
    auto* items = ExpressionList::make(pool);
    items->add(ConstantExpression::make(pool, 1));
    auto expr = SubscriptExpression::make(
        pool, ListExpression::make(pool, items), ConstantExpression::make(pool, "test"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_FALSE(visitor.ok());
  }
  {
    auto expr = SubscriptExpression::make(
        pool,
        ConstantExpression::make(pool, Value(Map({{"k", "v"}, {"k1", "v1"}}))),
        ConstantExpression::make(pool, 1));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_FALSE(visitor.ok());
  }
}

TEST_F(DeduceTypeVisitorTest, Attribute) {
  {
    auto expr = AttributeExpression::make(
        pool,
        ConstantExpression::make(pool, Value(Map({{"k", "v"}, {"k1", "v1"}}))),
        ConstantExpression::make(pool, "a"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = AttributeExpression::make(pool,
                                          ConstantExpression::make(pool, Value(Vertex("vid", {}))),
                                          ConstantExpression::make(pool, "a"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = AttributeExpression::make(
        pool,
        ConstantExpression::make(pool, Value(Edge("v1", "v2", 1, "edge", 0, {}))),
        ConstantExpression::make(pool, "a"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = AttributeExpression::make(pool,
                                          ConstantExpression::make(pool, Value::kNullValue),
                                          ConstantExpression::make(pool, "a"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }
  {
    auto expr = AttributeExpression::make(pool,
                                          ConstantExpression::make(pool, Value::kNullValue),
                                          ConstantExpression::make(pool, Value::kNullValue));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.type(), Value::Type::__EMPTY__);
  }

  // exceptions
  {
    auto expr = AttributeExpression::make(
        pool, ConstantExpression::make(pool, "test"), ConstantExpression::make(pool, "a"));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_FALSE(visitor.ok());
  }
  {
    auto expr = AttributeExpression::make(
        pool,
        ConstantExpression::make(pool, Value(Map({{"k", "v"}, {"k1", "v1"}}))),
        ConstantExpression::make(pool, 1));

    DeduceTypeVisitor visitor(emptyInput_, -1);
    expr->accept(&visitor);
    EXPECT_FALSE(visitor.ok());
  }
}
}  // namespace graph
}  // namespace nebula
