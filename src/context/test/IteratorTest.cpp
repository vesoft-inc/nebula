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

TEST(IteratorTest, GetNeighbor) {
    DataSet ds1;
    ds1.colNames = {kVid,
                    "_stats",
                    "_tag:tag1:prop1:prop2",
                    "_edge:+edge1:prop1:prop2:_dst:_rank",
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
                    "_edge:-edge2:prop1:prop2:_dst:_rank",
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
        size_t i = 0;
        while (iter.valid()) {
            ++i;
            if (i % 2 == 0) {
                iter.erase();
            } else {
                iter.next();
            }
        }
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

        for (iter.reset(10); iter.valid(); iter.next()) {
            count--;
        }
        EXPECT_EQ(count, 10);
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
                edge.type = 0;
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
                edge.type = 0;
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
                edge.type = 0;
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
                edge.type = 0;
                edge.src = folly::to<std::string>(i);
                edge.dst = "2";
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_TRUE(iter.valid_);
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
        GetNeighborsIter iter(std::move(val));
        EXPECT_FALSE(iter.valid_);
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

TEST(IteratorTest, Join) {
    DataSet ds1;
    ds1.colNames = {kVid, "tag_prop", "edge_prop", kDst};
    auto val1 = std::make_shared<Value>(ds1);
    SequentialIter iter1(val1);

    DataSet ds2;
    ds2.colNames = {"src", "dst"};
    auto val2 = std::make_shared<Value>(ds2);
    SequentialIter iter2(val2);

    Row row1;
    row1.values = {"1", 1, 2, "2"};
    Row row2;
    row2.values = {"3", "4"};
    JoinIter joinIter;
    joinIter.joinIndex(&iter1, &iter2);
    EXPECT_EQ(joinIter.getColIdxIndices().size(), 6);
    EXPECT_EQ(joinIter.getColIdxIndices().size(), 6);
    joinIter.addRow(JoinIter::JoinLogicalRow({ &row1, &row2 }, 6, &joinIter.getColIdxIndices()));
    joinIter.addRow(JoinIter::JoinLogicalRow({ &row1, &row2 }, 6, &joinIter.getColIdxIndices()));

    for (; joinIter.valid(); joinIter.next()) {
        const auto& row = *joinIter.row();
        EXPECT_EQ(row.size(), 6);
        std::vector<Value> result;
        for (size_t i = 0; i < 6; ++i) {
            result.emplace_back(row[i]);
        }
        EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4"}));
    }

    for (joinIter.reset(); joinIter.valid(); joinIter.next()) {
        const auto& row = *joinIter.row();
        EXPECT_EQ(row.size(), 6);
        std::vector<Value> result;
        result.emplace_back(joinIter.getColumn(kVid));
        result.emplace_back(joinIter.getColumn("tag_prop"));
        result.emplace_back(joinIter.getColumn("edge_prop"));
        result.emplace_back(joinIter.getColumn(kDst));
        result.emplace_back(joinIter.getColumn("src"));
        result.emplace_back(joinIter.getColumn("dst"));
        EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4"}));
    }

    {
        // The iterator and executors will not handle the duplicate columns,
        // so the duplicate column will be covered by later one.
        JoinIter joinIter1;
        joinIter1.joinIndex(&iter2, &joinIter);
        EXPECT_EQ(joinIter.getColIndices().size(), 6);
    }

    {
        DataSet ds3;
        ds3.colNames = {"tag_prop1", "edge_prop1"};
        auto val3 = std::make_shared<Value>(ds3);
        SequentialIter iter3(val3);

        Row row3;
        row3.values = {"5", "6"};

        JoinIter joinIter2;
        joinIter2.joinIndex(&iter3, &joinIter);
        EXPECT_EQ(joinIter2.getColIndices().size(), 8);
        EXPECT_EQ(joinIter2.getColIdxIndices().size(), 8);
        joinIter2.addRow(JoinIter::JoinLogicalRow({ &row3, &row1, &row2}, 8,
                                                &joinIter2.getColIdxIndices()));
        joinIter2.addRow(JoinIter::JoinLogicalRow({ &row3, &row1, &row2}, 8,
                                                &joinIter2.getColIdxIndices()));

        for (; joinIter2.valid(); joinIter2.next()) {
            const auto& row = *joinIter2.row();
            EXPECT_EQ(row.size(), 8);
            std::vector<Value> result;
            for (size_t i = 0; i < 8; ++i) {
                result.emplace_back(row[i]);
            }
            EXPECT_EQ(result, std::vector<Value>({"5", "6", "1", 1, 2, "2", "3", "4"}));
        }

        for (joinIter2.reset(); joinIter2.valid(); joinIter2.next()) {
            const auto& row = *joinIter2.row();
            EXPECT_EQ(row.size(), 8);
            std::vector<Value> result;
            result.emplace_back(joinIter2.getColumn(kVid));
            result.emplace_back(joinIter2.getColumn("tag_prop"));
            result.emplace_back(joinIter2.getColumn("edge_prop"));
            result.emplace_back(joinIter2.getColumn(kDst));
            result.emplace_back(joinIter2.getColumn("src"));
            result.emplace_back(joinIter2.getColumn("dst"));
            result.emplace_back(joinIter2.getColumn("tag_prop1"));
            result.emplace_back(joinIter2.getColumn("edge_prop1"));
            EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4", "5", "6"}));
        }
    }
    {
        DataSet ds3;
        ds3.colNames = {"tag_prop1", "edge_prop1"};
        auto val3 = std::make_shared<Value>(ds3);
        SequentialIter iter3(val3);

        Row row3;
        row3.values = {"5", "6"};

        JoinIter joinIter2;
        joinIter2.joinIndex(&joinIter, &iter3);
        EXPECT_EQ(joinIter2.getColIndices().size(), 8);
        EXPECT_EQ(joinIter2.getColIdxIndices().size(), 8);
        joinIter2.addRow(JoinIter::JoinLogicalRow({ &row1, &row2, &row3 }, 8,
                                                &joinIter2.getColIdxIndices()));
        joinIter2.addRow(JoinIter::JoinLogicalRow({ &row1, &row2, &row3 }, 8,
                                                &joinIter2.getColIdxIndices()));

        for (; joinIter2.valid(); joinIter2.next()) {
            const auto& row = *joinIter2.row();
            EXPECT_EQ(row.size(), 8);
            std::vector<Value> result;
            for (size_t i = 0; i < 8; ++i) {
                result.emplace_back(row[i]);
            }
            EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4", "5", "6"}));
        }

        for (joinIter2.reset(); joinIter2.valid(); joinIter2.next()) {
            const auto& row = *joinIter2.row();
            EXPECT_EQ(row.size(), 8);
            std::vector<Value> result;
            result.emplace_back(joinIter2.getColumn(kVid));
            result.emplace_back(joinIter2.getColumn("tag_prop"));
            result.emplace_back(joinIter2.getColumn("edge_prop"));
            result.emplace_back(joinIter2.getColumn(kDst));
            result.emplace_back(joinIter2.getColumn("src"));
            result.emplace_back(joinIter2.getColumn("dst"));
            result.emplace_back(joinIter2.getColumn("tag_prop1"));
            result.emplace_back(joinIter2.getColumn("edge_prop1"));
            EXPECT_EQ(result, std::vector<Value>({"1", 1, 2, "2", "3", "4", "5", "6"}));
        }
    }
}
}  // namespace graph
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
