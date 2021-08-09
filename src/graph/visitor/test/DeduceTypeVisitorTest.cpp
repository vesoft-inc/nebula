/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "visitor/DeduceTypeVisitor.h"
#include "visitor/test/VisitorTestBase.h"

#include <gtest/gtest.h>

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
        auto expr = SubscriptExpression::make(pool,
                                               ConstantExpression::make(pool, Value::kNullValue),
                                               ConstantExpression::make(pool, 1));

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
        auto expr =
            AttributeExpression::make(pool,
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
}   // namespace graph
}   // namespace nebula
