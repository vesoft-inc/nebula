/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class AttributeExpressionTest : public ExpressionTest {};

TEST_F(AttributeExpressionTest, MapAttribute) {
    // {"key1":1, "key2":2, "key3":3}.key1
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 1))
            .add("key2", ConstantExpression::make(&pool, 2))
            .add("key3", ConstantExpression::make(&pool, 3));
        auto *map = MapExpression::make(&pool, items);
        auto *key = LabelExpression::make(&pool, "key1");
        auto expr = AttributeExpression::make(&pool, map, key);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
}

TEST_F(AttributeExpressionTest, EdgeAttribute) {
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
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = LabelExpression::make(&pool, "Rocky");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Raccoon", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = LabelExpression::make(&pool, kType);
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("type", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = LabelExpression::make(&pool, kSrc);
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("src", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = LabelExpression::make(&pool, kDst);
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("dst", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = LabelExpression::make(&pool, kRank);
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(123, value.getInt());
    }
}

TEST_F(AttributeExpressionTest, VertexAttribute) {
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
        auto *left = ConstantExpression::make(&pool, Value(vertex));
        auto *right = LabelExpression::make(&pool, "Mull");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Kintyre", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(vertex));
        auto *right = LabelExpression::make(&pool, "Bip");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Bop", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(vertex));
        auto *right = LabelExpression::make(&pool, "Venus");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Mars", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(vertex));
        auto *right = LabelExpression::make(&pool, "_vid");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("vid", value.getStr());
    }
}

TEST_F(AttributeExpressionTest, DateTimeAttribute) {
    DateTime dt(2021, 7, 19, 2, 7, 57, 0);
    Date d(2021, 7, 19);
    Time t(2, 7, 57, 0);
    {
        auto *left = ConstantExpression::make(&pool, Value(dt));
        auto *right = LabelExpression::make(&pool, "not exist attribute");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(Value::kNullUnknownProp, value);
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(dt));
        auto *right = LabelExpression::make(&pool, "year");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(Value(2021), value);
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(d));
        auto *right = LabelExpression::make(&pool, "not exist attribute");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(Value::kNullUnknownProp, value);
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(d));
        auto *right = LabelExpression::make(&pool, "day");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(Value(19), value);
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(t));
        auto *right = LabelExpression::make(&pool, "not exist attribute");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(Value::kNullUnknownProp, value);
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(t));
        auto *right = LabelExpression::make(&pool, "minute");
        auto expr = AttributeExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_EQ(Value(7), value);
    }
}

}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
