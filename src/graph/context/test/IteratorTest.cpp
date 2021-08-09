/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "context/Iterator.h"
#include "common/datatypes/Vertex.h"
#include "common/datatypes/Edge.h"

namespace nebula {
namespace graph {

TEST(IteratorTest, Default) {
    auto constant = std::make_shared<Value>(1);
    DefaultIter iter(constant);
    EXPECT_EQ(iter.size(), 1);
    for (; iter.valid(); iter.next()) {
        EXPECT_EQ(*iter.valuePtr(), *constant);
    }
}

TEST(IteratorTest, Sequential) {
    DataSet ds;
    ds.colNames = {"col1", "col2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(i);
        row.values.emplace_back(folly::to<std::string>(i));
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

TEST(IteratorTest, GetNeighborNoEdge) {
    DataSet ds1;
    ds1.colNames = {kVid, "_stats", "_tag:tag1:prop1:prop2", "_expr"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        // _vid
        row.values.emplace_back(folly::to<std::string>(i));
        // _stats = empty
        row.values.emplace_back(Value());
        // tag
        List tag;
        tag.values.emplace_back(0);
        tag.values.emplace_back(1);
        row.values.emplace_back(Value(tag));
        // _expr = empty
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
    }

    DataSet ds2;
    ds2.colNames = {kVid, "_stats", "_tag:tag2:prop1:prop2", "_expr"};
    for (auto i = 10; i < 20; ++i) {
        Row row;
        // _vid
        row.values.emplace_back(folly::to<std::string>(i));
        // _stats = empty
        row.values.emplace_back(Value());
        // tag
        List tag;
        tag.values.emplace_back(0);
        tag.values.emplace_back(1);
        row.values.emplace_back(Value(tag));
        // _expr = empty
        row.values.emplace_back(Value());
        ds2.rows.emplace_back(std::move(row));
    }

    List datasets;
    datasets.values.emplace_back(std::move(ds1));
    datasets.values.emplace_back(std::move(ds2));
    auto val = std::make_shared<Value>(std::move(datasets));

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected = {"0",  "1",  "2",  "3",  "4",  "5",  "6",  "7",  "8",  "9",
                                       "10", "11", "12", "13", "14", "15", "16", "17", "18", "19"};
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getColumn(kVid));
        }
        EXPECT_EQ(expected, result);
    }

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 10, 0);
        expected.insert(expected.end(), 10, Value());
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getTagProp("tag1", "prop1"));
        }
        EXPECT_EQ(result.size(), 20);
        EXPECT_EQ(expected, result);
    }
}

TEST(IteratorTest, GetNeighbor) {
    DataSet ds1;
    ds1.colNames = {kVid,
                    "_stats",
                    "_tag:tag1:prop1:prop2",
                    "_edge:+edge1:prop1:prop2:_dst:_type:_rank",
                    "_expr"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        // _vid
        row.values.emplace_back(folly::to<std::string>(i));
        // _stats = empty
        row.values.emplace_back(Value());
        // tag
        List tag;
        tag.values.emplace_back(0);
        tag.values.emplace_back(1);
        row.values.emplace_back(Value(tag));
        // edges
        List edges;
        for (auto j = 0; j < 2; ++j) {
            List edge;
            for (auto k = 0; k < 2; ++k) {
                edge.values.emplace_back(k);
            }
            edge.values.emplace_back("2");
            edge.values.emplace_back(1);
            edge.values.emplace_back(j);
            edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        // _expr = empty
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
    }

    DataSet ds2;
    ds2.colNames = {kVid,
                    "_stats",
                    "_tag:tag2:prop1:prop2",
                    "_edge:-edge2:prop1:prop2:_dst:_type:_rank",
                    "_expr"};
    for (auto i = 10; i < 20; ++i) {
        Row row;
        // _vid
        row.values.emplace_back(folly::to<std::string>(i));
        // _stats = empty
        row.values.emplace_back(Value());
        // tag
        List tag;
        tag.values.emplace_back(0);
        tag.values.emplace_back(1);
        row.values.emplace_back(Value(tag));
        // edges
        List edges;
        for (auto j = 0; j < 2; ++j) {
            List edge;
            for (auto k = 0; k < 2; ++k) {
                edge.values.emplace_back(k);
            }
            edge.values.emplace_back("2");
            edge.values.emplace_back(-2);
            edge.values.emplace_back(j);
            edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        // _expr = empty
        row.values.emplace_back(Value());
        ds2.rows.emplace_back(std::move(row));
    }

    List datasets;
    datasets.values.emplace_back(std::move(ds1));
    datasets.values.emplace_back(std::move(ds2));
    auto val = std::make_shared<Value>(std::move(datasets));

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected =
            {"0", "0", "1", "1", "2", "2", "3", "3", "4", "4",
             "5", "5", "6", "6", "7", "7", "8", "8", "9", "9",
             "10", "10", "11", "11", "12", "12", "13", "13", "14", "14",
             "15", "15", "16", "16", "17", "17", "18", "18", "19", "19"};
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getColumn(kVid));
        }
        EXPECT_EQ(expected, result);
    }

    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        expected.insert(expected.end(), 20, 0);
        expected.insert(expected.end(), 20, Value());
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
        expected.insert(expected.end(), 20, Value());
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
        expected.insert(expected.end(), 20, Value());
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
        expected.insert(expected.end(), 20, Value());
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
        expected.insert(expected.end(), 20, Value());
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
        size_t i = 0;
        while (iter.valid()) {
            ++i;
            if (i % 2 == 0) {
                iter.erase();
            } else {
                iter.next();
            }
        }
        EXPECT_EQ(i, 40);

        std::vector<Value> expected =
                {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "10", "11", "12", "13", "14", "15", "16", "17", "18", "19"};
        std::vector<Value> result;

        int count = 0;
        for (iter.reset(); iter.valid(); iter.next()) {
            result.emplace_back(iter.getColumn(kVid));
            count++;
        }
        EXPECT_EQ(result.size(), 20);
        EXPECT_EQ(expected, result);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        Tag tag1;
        tag1.name = "tag1";
        tag1.props = {{"prop1", 0}, {"prop2", 1}};
        for (size_t i = 0; i < 10; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            vertex.tags.emplace_back(tag1);
            expected.emplace_back(vertex);
            expected.emplace_back(std::move(vertex));
        }
        Tag tag2;
        tag2.name = "tag2";
        tag2.props = {{"prop1", 0}, {"prop2", 1}};
        for (size_t i = 10; i < 20; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            vertex.tags.emplace_back(tag2);
            expected.emplace_back(vertex);
            expected.emplace_back(std::move(vertex));
        }
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            auto v = iter.getVertex();
            result.emplace_back(std::move(v));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(result, expected);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        for (size_t i = 0; i < 10; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                EdgeRanking ranking = static_cast<int64_t>(j);
                Edge edge;
                edge.name = "edge1";
                edge.type = 1;
                edge.src = folly::to<std::string>(i);
                edge.dst = "2";
                edge.ranking = ranking;
                edge.props = {{"prop1", 0}, {"prop2", 1}};
                expected.emplace_back(std::move(edge));
            }
        }
        for (size_t i = 10; i < 20; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                EdgeRanking ranking = static_cast<int64_t>(j);
                Edge edge;
                edge.name = "edge2";
                edge.type = -2;
                edge.src = folly::to<std::string>(i);
                edge.dst = "2";
                edge.ranking = ranking;
                edge.props = {{"prop1", 0}, {"prop2", 1}};
                expected.emplace_back(std::move(edge));
            }
        }
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            auto e = iter.getEdge();
            result.emplace_back(std::move(e));
        }
        EXPECT_EQ(result.size(), 40);
        EXPECT_EQ(result, expected);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        Tag tag1;
        tag1.name = "tag1";
        tag1.props = {{"prop1", 0}, {"prop2", 1}};
        for (size_t i = 0; i < 10; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            vertex.tags.emplace_back(tag1);
            expected.emplace_back(std::move(vertex));
        }
        Tag tag2;
        tag2.name = "tag2";
        tag2.props = {{"prop1", 0}, {"prop2", 1}};
        for (size_t i = 10; i < 20; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            vertex.tags.emplace_back(tag2);
            expected.emplace_back(std::move(vertex));
        }
        List result = iter.getVertices();
        EXPECT_EQ(result.values.size(), 20);
        EXPECT_EQ(result.values, expected);

        result = iter.getVertices();
        EXPECT_EQ(result.values.size(), 20);
        EXPECT_EQ(result.values, expected);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> expected;
        for (size_t i = 0; i < 10; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                EdgeRanking ranking = static_cast<int64_t>(j);
                Edge edge;
                edge.name = "edge1";
                edge.type = 1;
                edge.src = folly::to<std::string>(i);
                edge.dst = "2";
                edge.ranking = ranking;
                edge.props = {{"prop1", 0}, {"prop2", 1}};
                expected.emplace_back(std::move(edge));
            }
        }
        for (size_t i = 10; i < 20; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                EdgeRanking ranking = static_cast<int64_t>(j);
                Edge edge;
                edge.name = "edge2";
                edge.type = 2;
                edge.src = "2";
                edge.dst = folly::to<std::string>(i);
                edge.ranking = ranking;
                edge.props = {{"prop1", 0}, {"prop2", 1}};
                expected.emplace_back(std::move(edge));
            }
        }
        List result = iter.getEdges();
        EXPECT_EQ(result.values.size(), 40);
        EXPECT_EQ(result.values, expected);

        result = iter.getEdges();
        EXPECT_EQ(result.values.size(), 40);
        EXPECT_EQ(result.values, expected);
    }
    // unstableErase
    {
        GetNeighborsIter iter(val);
        size_t i = 0;
        while (iter.valid()) {
            ++i;
            if (i % 2 == 0) {
                iter.unstableErase();
            } else {
                iter.next();
            }
        }
        EXPECT_EQ(i, 40);
        std::vector<Value> result;

        int count = 0;
        for (iter.reset(); iter.valid(); iter.next()) {
            result.emplace_back(iter.getColumn(kVid));
            count++;
        }
        EXPECT_EQ(result.size(), 20);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            auto v = iter.getVertex();
            result.emplace_back(std::move(v));
        }
        EXPECT_EQ(result.size(), 40);
    }
    {
        GetNeighborsIter iter(val);
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            auto e = iter.getEdge();
            result.emplace_back(std::move(e));
        }
        EXPECT_EQ(result.size(), 40);
    }
    {
        GetNeighborsIter iter(val);
        List result = iter.getVertices();
        EXPECT_EQ(result.values.size(), 20);

        result = iter.getVertices();
        EXPECT_EQ(result.values.size(), 20);
    }
    {
        GetNeighborsIter iter(val);
        List result = iter.getEdges();
        EXPECT_EQ(result.values.size(), 40);

        result = iter.getEdges();
        EXPECT_EQ(result.values.size(), 40);
    }
}

TEST(IteratorTest, TestHead) {
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_TRUE(status.ok());
    }

    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_TRUE(status.ok());
    }
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_TRUE(status.ok());
    }
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_TRUE(status.ok());
    }
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1",
                        "_edge:+edge1:",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_TRUE(status.ok());
    }

    {
        // no _vid
        DataSet ds;
        ds.colNames = {"_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_FALSE(status.ok());
    }
    {
        // no _stats
        DataSet ds;
        ds.colNames = {kVid,
                        "_tag:tag1:prop1:prop2",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_FALSE(status.ok());
    }
    {
        // no _expr
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:+edge1:prop1:prop2:_dst:_rank"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_FALSE(status.ok());
    }
    {
        // no +/- before edge name
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_FALSE(status.ok());
    }
    // no prop
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(val);
        EXPECT_FALSE(iter.valid_);
    }
    // no prop
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1",
                        "_edge:+edge1:prop1:prop2:_dst:_rank",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(val);
        EXPECT_FALSE(iter.valid_);
    }
    {
        DataSet ds;
        ds.colNames = {kVid,
                        "_stats",
                        "_tag:tag1:prop1:prop2",
                        "_edge:::",
                        "_expr"};
        List datasets;
        datasets.values.emplace_back(std::move(ds));
        auto val = std::make_shared<Value>(std::move(datasets));
        GetNeighborsIter iter(nullptr);
        auto status = iter.processList(val);
        EXPECT_FALSE(status.ok());
    }
}

TEST(IteratorTest, EraseRange) {
    // Sequential iterator
    {
        DataSet ds({"col1", "col2"});
        for (auto i = 0; i < 10; ++i) {
            ds.rows.emplace_back(Row({i, folly::to<std::string>(i)}));
        }
        // erase out of range pos
        {
            auto val = std::make_shared<Value>(ds);
            SequentialIter iter(val);
            iter.eraseRange(5, 11);
            ASSERT_EQ(iter.size(), 5);
            auto i = 0;
            for (; iter.valid(); iter.next()) {
                ASSERT_EQ(iter.getColumn("col1"), i);
                ASSERT_EQ(iter.getColumn("col2"), folly::to<std::string>(i));
                ++i;
            }
        }
        // erase in range
        {
            auto val = std::make_shared<Value>(ds);
            SequentialIter iter(val);
            iter.eraseRange(0, 10);
            ASSERT_EQ(iter.size(), 0);
        }
        // erase part
        {
            auto val = std::make_shared<Value>(ds);
            SequentialIter iter(val);
            iter.eraseRange(0, 5);
            EXPECT_EQ(iter.size(), 5);
            auto i = 5;
            for (; iter.valid(); iter.next()) {
                ASSERT_EQ(iter.getColumn("col1"), i);
                ASSERT_EQ(iter.getColumn("col2"), folly::to<std::string>(i));
                ++i;
            }
        }
    }
}

TEST(IteratorTest, VertexProp) {
    DataSet ds;
    ds.colNames = {kVid, "tag1.prop1", "tag2.prop1", "tag2.prop2", "tag3.prop1", "tag3.prop2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        // _vid
        row.values.emplace_back(folly::to<std::string>(i));

        row.values.emplace_back(11);
        row.values.emplace_back(Value());
        row.values.emplace_back(Value());
        row.values.emplace_back(31);
        row.values.emplace_back(32);

        ds.rows.emplace_back(std::move(row));
    }
    auto val = std::make_shared<Value>(std::move(ds));
    {
        PropIter iter(val);
        std::vector<Value> expected =
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getColumn(kVid));
        }
        EXPECT_EQ(expected, result);
    }
    {
        PropIter iter(val);
        std::vector<Value> expected;
        for (size_t i = 0; i < 10; ++i) {
            Vertex vertex;
            vertex.vid = folly::to<std::string>(i);
            Tag tag1;
            tag1.name = "tag1";
            tag1.props = {{"prop1", 11}};
            Tag tag3;
            tag3.name = "tag3";
            tag3.props = {{"prop1", 31}, {"prop2", 32}};
            vertex.tags.emplace_back(tag3);
            vertex.tags.emplace_back(tag1);
            expected.emplace_back(std::move(vertex));
        }
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            auto v = iter.getVertex();
            result.emplace_back(std::move(v));
        }
        EXPECT_EQ(result.size(), 10);
        EXPECT_EQ(result, expected);
    }
}

TEST(IteratorTest, EdgeProp) {
    DataSet ds;
    ds.colNames = {"like._src",
                   "like._type",
                   "like._rank",
                   "like._dst",
                   "like.prop1",
                   "like.prop2",
                   "serve.prop1",
                   "serve.prop2"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(folly::to<std::string>(i));
        row.values.emplace_back(2);
        row.values.emplace_back(0);
        row.values.emplace_back(folly::to<std::string>(i * 2 + 3));
        row.values.emplace_back("hello");
        row.values.emplace_back("world");
        row.values.emplace_back(Value());
        row.values.emplace_back(Value());
        ds.rows.emplace_back(std::move(row));
    }
    auto val = std::make_shared<Value>(std::move(ds));
    {
        PropIter iter(val);
        std::vector<Value> expected =
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            result.emplace_back(iter.getEdgeProp("like", kSrc));
        }
        EXPECT_EQ(expected, result);
    }
    {
        PropIter iter(val);
        std::vector<Value> expected;
        for (size_t i = 0; i < 10; ++i) {
            Edge edge;
            edge.src = folly::to<std::string>(i);
            edge.dst = folly::to<std::string>(i * 2 + 3);
            edge.type = 2;
            edge.ranking = 0;
            edge.name = "like";
            edge.props = {{"prop1", "hello"}, {"prop2", "world"}};
            expected.emplace_back(std::move(edge));
        }
        std::vector<Value> result;
        for (; iter.valid(); iter.next()) {
            auto v = iter.getEdge();
            result.emplace_back(std::move(v));
        }
        EXPECT_EQ(result.size(), 10);
        EXPECT_EQ(result, expected);
    }
}

TEST(IteratorTest, EraseBySwap) {
    DataSet ds;
    ds.colNames = {"col1", "col2"};
    for (auto i = 0; i < 3; ++i) {
        Row row;
        row.values.emplace_back(i);
        row.values.emplace_back(folly::to<std::string>(i));
        ds.rows.emplace_back(std::move(row));
    }
    auto val = std::make_shared<Value>(std::move(ds));
    SequentialIter iter(val);
    EXPECT_EQ(iter.size(), 3);
    iter.unstableErase();
    EXPECT_EQ(iter.size(), 2);


    std::vector<Row> expected;
    {
        Row row;
        row.values.emplace_back(2);
        row.values.emplace_back("2");
        expected.emplace_back(std::move(row));
    }
    {
        Row row;
        row.values.emplace_back(1);
        row.values.emplace_back("1");
        expected.emplace_back(std::move(row));
    }
    std::vector<Row> result;
    for (; iter.valid(); iter.next()) {
        Row row;
        row.values.emplace_back(iter.getColumn("col1"));
        row.values.emplace_back(iter.getColumn("col2"));
        result.emplace_back(std::move(row));
    }
    EXPECT_EQ(result, expected);
}
}  // namespace graph
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
