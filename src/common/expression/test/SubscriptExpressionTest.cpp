/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class SubscriptExpressionTest : public ExpressionTest {};

TEST_F(SubscriptExpressionTest, DataSetSubscript) {
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
        auto *dataset = ConstantExpression::make(&pool, ds);
        auto *rowIndex = ConstantExpression::make(&pool, 0);
        auto expr = SubscriptExpression::make(&pool, dataset, rowIndex);
        auto value = Expression::eval(expr, gExpCtxt);
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
        auto *dataset = ConstantExpression::make(&pool, ds);
        auto *rowIndex = ConstantExpression::make(&pool, 0);
        auto *colIndex = ConstantExpression::make(&pool, 1);
        auto expr = SubscriptExpression::make(
            &pool, SubscriptExpression::make(&pool, dataset, rowIndex), colIndex);
        auto value = Expression::eval(expr, gExpCtxt);
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
        auto *dataset = ConstantExpression::make(&pool, ds);
        auto *rowIndex = ConstantExpression::make(&pool, -1);
        auto expr = SubscriptExpression::make(&pool, dataset, rowIndex);
        auto value = Expression::eval(expr, gExpCtxt);
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
        auto *dataset = ConstantExpression::make(&pool, ds);
        auto *rowIndex = ConstantExpression::make(&pool, 0);
        auto *colIndex = ConstantExpression::make(&pool, 5);
        auto expr = SubscriptExpression::make(
            &pool, SubscriptExpression::make(&pool, dataset, rowIndex), colIndex);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
}

TEST_F(SubscriptExpressionTest, ListSubscript) {
    // [1,2,3,4][0]
    {
        auto *items = ExpressionList::make(&pool);
        (*items)
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 3))
            .add(ConstantExpression::make(&pool, 4));
        auto *list = ListExpression::make(&pool, items);
        auto *index = ConstantExpression::make(&pool, 0);
        auto expr = SubscriptExpression::make(&pool, list, index);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
    // [1,2,3,4][3]
    {
        auto *items = ExpressionList::make(&pool);
        (*items)
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 3))
            .add(ConstantExpression::make(&pool, 4));
        auto *list = ListExpression::make(&pool, items);
        auto *index = ConstantExpression::make(&pool, 3);
        auto expr = SubscriptExpression::make(&pool, list, index);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(4, value.getInt());
    }
    // [1,2,3,4][4]
    {
        auto *items = ExpressionList::make(&pool);
        (*items)
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 3))
            .add(ConstantExpression::make(&pool, 4));
        auto *list = ListExpression::make(&pool, items);
        auto *index = ConstantExpression::make(&pool, 4);
        auto expr = SubscriptExpression::make(&pool, list, index);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
    // [1,2,3,4][-1]
    {
        auto *items = ExpressionList::make(&pool);
        (*items)
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 3))
            .add(ConstantExpression::make(&pool, 4));
        auto *list = ListExpression::make(&pool, items);
        auto *index = ConstantExpression::make(&pool, -1);
        auto expr = SubscriptExpression::make(&pool, list, index);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(4, value.getInt());
    }
    // [1,2,3,4][-4]
    {
        auto *items = ExpressionList::make(&pool);
        (*items)
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 3))
            .add(ConstantExpression::make(&pool, 4));
        auto *list = ListExpression::make(&pool, items);
        auto *index = ConstantExpression::make(&pool, -4);
        auto expr = SubscriptExpression::make(&pool, list, index);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
    // [1,2,3,4][-5]
    {
        auto *items = ExpressionList::make(&pool);
        (*items)
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 3))
            .add(ConstantExpression::make(&pool, 4));
        auto *list = ListExpression::make(&pool, items);
        auto *index = ConstantExpression::make(&pool, -5);
        auto expr = SubscriptExpression::make(&pool, list, index);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
    // [1,2,3,4]["0"]
    {
        auto *items = ExpressionList::make(&pool);
        (*items)
            .add(ConstantExpression::make(&pool, 1))
            .add(ConstantExpression::make(&pool, 2))
            .add(ConstantExpression::make(&pool, 3))
            .add(ConstantExpression::make(&pool, 4));
        auto *list = ListExpression::make(&pool, items);
        auto *index = ConstantExpression::make(&pool, "0");
        auto expr = SubscriptExpression::make(&pool, list, index);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
    // 1[0]
    {
        auto *integer = ConstantExpression::make(&pool, 1);
        auto *index = ConstantExpression::make(&pool, 0);
        auto expr = SubscriptExpression::make(&pool, integer, index);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isBadNull());
    }
}

TEST_F(SubscriptExpressionTest, ListSubscriptRange) {
    auto *items = ExpressionList::make(&pool);
    (*items)
        .add(ConstantExpression::make(&pool, 0))
        .add(ConstantExpression::make(&pool, 1))
        .add(ConstantExpression::make(&pool, 2))
        .add(ConstantExpression::make(&pool, 3))
        .add(ConstantExpression::make(&pool, 4))
        .add(ConstantExpression::make(&pool, 5));
    auto list = ListExpression::make(&pool, items);
    // [0,1,2,3,4,5][0..] => [0,1,2,3,4,5]
    {
        auto *lo = ConstantExpression::make(&pool, 0);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, nullptr);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List({0, 1, 2, 3, 4, 5}), value.getList());
    }
    // [0,1,2,3,4,5][0..0] => []
    {
        auto *lo = ConstantExpression::make(&pool, 0);
        auto *hi = ConstantExpression::make(&pool, 0);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List(), value.getList());
    }
    // [0,1,2,3,4,5][0..10] => [0,1,2,3,4,5]
    {
        auto *lo = ConstantExpression::make(&pool, 0);
        auto *hi = ConstantExpression::make(&pool, 10);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List({0, 1, 2, 3, 4, 5}), value.getList());
    }
    // [0,1,2,3,4,5][3..2] => []
    {
        auto *lo = ConstantExpression::make(&pool, 3);
        auto *hi = ConstantExpression::make(&pool, 2);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List(), value.getList());
    }
    // [0,1,2,3,4,5][..-1] => [0,1,2,3,4]
    {
        auto *hi = ConstantExpression::make(&pool, -1);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), nullptr, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List({0, 1, 2, 3, 4}), value.getList());
    }
    // [0,1,2,3,4,5][-1..-1] => []
    {
        auto *lo = ConstantExpression::make(&pool, -1);
        auto *hi = ConstantExpression::make(&pool, -1);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List(), value.getList());
    }
    // [0,1,2,3,4,5][-10..-1] => [0,1,2,3,4]
    {
        auto *lo = ConstantExpression::make(&pool, -10);
        auto *hi = ConstantExpression::make(&pool, -1);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List({0, 1, 2, 3, 4}), value.getList());
    }
    // [0,1,2,3,4,5][-2..-3] => []
    {
        auto *lo = ConstantExpression::make(&pool, -2);
        auto *hi = ConstantExpression::make(&pool, -3);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List(), value.getList());
    }
    // [0,1,2,3,4,5][2..-3] => [2]
    {
        auto *lo = ConstantExpression::make(&pool, 2);
        auto *hi = ConstantExpression::make(&pool, -3);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List({2}), value.getList());
    }
    // [0,1,2,3,4,5][-2..3] => []
    {
        auto *lo = ConstantExpression::make(&pool, -2);
        auto *hi = ConstantExpression::make(&pool, 3);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List(), value.getList());
    }
    // [0,1,2,3,4,5][-2..] => [4,5]
    {
        auto *lo = ConstantExpression::make(&pool, -2);
        auto *hi = ConstantExpression::make(&pool, 3);
        auto expr = SubscriptRangeExpression::make(&pool, list->clone(), lo, hi);
        auto value = Expression::eval(expr, gExpCtxt);
        EXPECT_TRUE(value.isList());
        EXPECT_EQ(List(), value.getList());
    }
}

TEST_F(SubscriptExpressionTest, MapSubscript) {
    // {"key1":1,"key2":2, "key3":3}["key1"]
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 1))
            .add("key2", ConstantExpression::make(&pool, 2))
            .add("key3", ConstantExpression::make(&pool, 3));
        auto *map = MapExpression::make(&pool, items);
        auto *key = ConstantExpression::make(&pool, "key1");
        auto expr = SubscriptExpression::make(&pool, map, key);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(1, value.getInt());
    }
    // {"key1":1,"key2":2, "key3":3}["key4"]
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 1))
            .add("key2", ConstantExpression::make(&pool, 2))
            .add("key3", ConstantExpression::make(&pool, 3));
        auto *map = MapExpression::make(&pool, items);
        auto *key = ConstantExpression::make(&pool, "key4");
        auto expr = SubscriptExpression::make(&pool, map, key);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isNull());
        ASSERT_TRUE(value.isBadNull());
    }
    // {"key1":1,"key2":2, "key3":3}[0]
    {
        auto *items = MapItemList::make(&pool);
        (*items)
            .add("key1", ConstantExpression::make(&pool, 1))
            .add("key2", ConstantExpression::make(&pool, 2))
            .add("key3", ConstantExpression::make(&pool, 3));
        auto *map = MapExpression::make(&pool, items);
        auto *key = ConstantExpression::make(&pool, 0);
        auto expr = SubscriptExpression::make(&pool, map, key);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isNull());
        ASSERT_FALSE(value.isBadNull());
    }
}

TEST_F(SubscriptExpressionTest, VertexSubscript) {
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
        auto *right = ConstantExpression::make(&pool, "Mull");
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Kintyre", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(vertex));
        auto *right = LabelExpression::make(&pool, "Bip");
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Bop", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(vertex));
        auto *right = LabelExpression::make(&pool, "Venus");
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Mars", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(vertex));
        auto *right = LabelExpression::make(&pool, "_vid");
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("vid", value.getStr());
    }
}

TEST_F(SubscriptExpressionTest, EdgeSubscript) {
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
        auto *right = ConstantExpression::make(&pool, "Rocky");
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Raccoon", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = ConstantExpression::make(&pool, kType);
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("type", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = ConstantExpression::make(&pool, kSrc);
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("src", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = ConstantExpression::make(&pool, kDst);
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("dst", value.getStr());
    }
    {
        auto *left = ConstantExpression::make(&pool, Value(edge));
        auto *right = ConstantExpression::make(&pool, kRank);
        auto expr = SubscriptExpression::make(&pool, left, right);
        auto value = Expression::eval(expr, gExpCtxt);
        ASSERT_TRUE(value.isInt());
        ASSERT_EQ(123, value.getInt());
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
