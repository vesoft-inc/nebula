/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "context/Iterator.h"

namespace nebula {
namespace graph {
TEST(IteratorTest, Default) {
    auto constant = std::make_shared<Value>(1);
    DefaultIter iter(constant);
    EXPECT_EQ(iter.size(), 1);
    for (; iter.valid(); iter.next()) {
        EXPECT_EQ(*iter, *constant);
    }
}

TEST(IteratorTest, Sequential) {
    DataSet ds;
    ds.colNames = {"col1", "col2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.columns.emplace_back(i);
        row.columns.emplace_back(folly::to<std::string>(i));
        ds.rows.emplace_back(std::move(row));
    }
    {
        auto val = std::make_shared<Value>(ds);
        SequentialIter iter(val);
        EXPECT_EQ(iter.size(), 10);
        auto i = 0;
        for (; iter.valid(); iter.next()) {
            EXPECT_EQ(iter.getColumn("col1"), i);
            EXPECT_EQ(iter.getColumn("col2"), folly::to<std::string>(i));
            ++i;
        }
    }
    {
        auto val = std::make_shared<Value>(ds);
        SequentialIter iter(val);
        auto copyIter1 = iter.copy();
        auto copyIter2 = copyIter1->copy();
        EXPECT_EQ(copyIter2->size(), 10);
        auto i = 0;
        for (; copyIter2->valid(); copyIter2->next()) {
            EXPECT_EQ(copyIter2->getColumn("col1"), i);
            EXPECT_EQ(copyIter2->getColumn("col2"), folly::to<std::string>(i));
            ++i;
        }
    }
    // erase
    {
        auto val = std::make_shared<Value>(std::move(ds));
        SequentialIter iter(val);
        EXPECT_EQ(iter.size(), 10);
        while (iter.valid()) {
            if (iter.getColumn("col1").getInt() % 2 == 0) {
                iter.erase();
            } else {
                iter.next();
            }
        }
        int32_t count = 0;
        for (iter.reset(); iter.valid(); iter.next()) {
            EXPECT_NE(iter.getColumn("col1").getInt() % 2, 0);
            count++;
        }

        for (iter.reset(1); iter.valid(); iter.next()) {
            count--;
        }
        EXPECT_EQ(count, 1);
    }
}

TEST(IteratorTest, GetNeighbor) {
    DataSet ds1;
    ds1.colNames = {"_vid", "_stats", "_tag:tag1:prop1:prop2", "_edge:edge1:prop1:prop2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        // _vid
        row.columns.emplace_back(i);
        // _stats = empty
        row.columns.emplace_back(Value());
        // tag
        List tag;
        tag.values.emplace_back(0);
        tag.values.emplace_back(1);
        row.columns.emplace_back(Value(tag));
        // edges
        List edges;
        for (auto j = 0; j < 2; ++j) {
            List edge;
            for (auto k = 0; k < 2; ++k) {
                edge.values.emplace_back(k);
            }
            edges.values.emplace_back(std::move(edge));
        }
        row.columns.emplace_back(edges);
        ds1.rows.emplace_back(std::move(row));
    }

    DataSet ds2;
    ds2.colNames = {"_vid", "_stats", "_tag:tag2:prop1:prop2", "_edge:edge2:prop1:prop2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        // _vid
        row.columns.emplace_back(i);
        // _stats = empty
        row.columns.emplace_back(Value());
        // tag
        List tag;
        tag.values.emplace_back(0);
        tag.values.emplace_back(1);
        row.columns.emplace_back(Value(tag));
        // edges
        List edges;
        for (auto j = 0; j < 2; ++j) {
            List edge;
            for (auto k = 0; k < 2; ++k) {
                edge.values.emplace_back(k);
            }
            edges.values.emplace_back(std::move(edge));
        }
        row.columns.emplace_back(edges);
        ds2.rows.emplace_back(std::move(row));
    }

    List datasets;
    datasets.values.emplace_back(std::move(ds1));
    datasets.values.emplace_back(std::move(ds2));
    auto val = std::make_shared<Value>(std::move(datasets));

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected =
            {0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9,
             0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9};
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getColumn("_vid"));
        }
        EXPECT_EQ(expected, result);
    }

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, 0);
        expected.insert(expected.end(), 20, Value(NullType::__NULL__));
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getTagProp("tag1", "prop1"));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, Value(NullType::__NULL__));
        expected.insert(expected.end(), 20, 0);
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getTagProp("tag2", "prop1"));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, 0);
        expected.insert(expected.end(), 20, Value(NullType::__NULL__));
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getEdgeProp("edge1", "prop1"));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, Value(NullType::__NULL__));
        expected.insert(expected.end(), 20, 0);
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getEdgeProp("edge2", "prop1"));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }
    {
        GetNeighborsIter iter(val);
        auto copyIter1 = iter.copy();
        auto copyIter2 = copyIter1->copy();
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, Value(NullType::__NULL__));
        expected.insert(expected.end(), 20, 0);
        std::vector<Value> result;
        for (; copyIter2->valid(); copyIter2->next()) {
            result.emplace_back(copyIter2->getEdgeProp("edge2", "prop1"));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(expected, result);
    }
    // erase
    {
        GetNeighborsIter iter(val);
        while (iter.valid()) {
            if (iter.getColumn("_vid").getInt() % 2 == 0) {
                iter.erase();
            } else {
                iter.next();
            }
        }
        std::vector<Value> expected =
                {1, 1, 3, 3, 5, 5, 7, 7, 9, 9,
                 1, 1, 3, 3, 5, 5, 7, 7, 9, 9};
        std::vector<Value> result;

        int count = 0;
        for (iter.reset(); iter.valid(); iter.next()) {
            result.emplace_back(iter.getColumn("_vid"));
            count++;
        }
        EXPECT_EQ(result.size(), 20);
        EXPECT_EQ(expected, result);

        for (iter.reset(10); iter.valid(); iter.next()) {
            count--;
        }
        EXPECT_EQ(count, 10);
    }
}

}  // namespace graph
}  // namespace nebula
