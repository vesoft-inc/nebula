/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/fs/TempDir.h"
#include "kvstore/RocksEngineConfig.h"
#include "storage/query/GetPropProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

cpp2::GetPropRequest buildVertexRequest(
        int32_t totalParts,
        const std::vector<VertexID>& vertices,
        const std::vector<std::pair<TagID, std::vector<std::string>>>& tags) {
    std::hash<std::string> hash;
    cpp2::GetPropRequest req;
    req.set_space_id(1);
    for (const auto& vertex : vertices) {
        PartitionID partId = (hash(vertex) % totalParts) + 1;
        nebula::Row row;
        row.values.emplace_back(vertex);
        (*req.parts_ref())[partId].emplace_back(std::move(row));
    }

    std::vector<cpp2::VertexProp> vertexProps;
    if (tags.empty()) {
        req.set_vertex_props(std::move(vertexProps));
    } else {
        for (const auto& tag : tags) {
            TagID tagId = tag.first;
            cpp2::VertexProp tagProp;
            tagProp.set_tag(tagId);
            for (const auto& prop : tag.second) {
                (*tagProp.props_ref()).emplace_back(std::move(prop));
            }
            vertexProps.emplace_back(std::move(tagProp));
        }
        req.set_vertex_props(std::move(vertexProps));
    }
    return req;
}

cpp2::GetPropRequest buildEdgeRequest(
        int32_t totalParts,
        const std::vector<cpp2::EdgeKey>& edgeKeys,
        const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges) {
    cpp2::GetPropRequest req;
    req.set_space_id(1);
    for (const auto& edge : edgeKeys) {
        PartitionID partId = (std::hash<Value>()(edge.get_src()) % totalParts) + 1;
        nebula::Row row;
        row.values.emplace_back(edge.get_src());
        row.values.emplace_back(edge.get_edge_type());
        row.values.emplace_back(edge.get_ranking());
        row.values.emplace_back(edge.get_dst());
        (*req.parts_ref())[partId].emplace_back(std::move(row));
    }

    std::vector<cpp2::EdgeProp> edgeProps;
    if (edges.empty()) {
        req.set_edge_props(std::move(edgeProps));
    } else {
        for (const auto& edge : edges) {
            EdgeType edgeType = edge.first;
            cpp2::EdgeProp edgeProp;
            edgeProp.set_type(edgeType);
            for (const auto& prop : edge.second) {
                (*edgeProp.props_ref()).emplace_back(std::move(prop));
            }
            edgeProps.emplace_back(std::move(edgeProp));
        }
        req.set_edge_props(std::move(edgeProps));
    }
    return req;
}

void verifyResult(const std::vector<nebula::Row>& expect,
                  const nebula::DataSet& dataSet) {
    ASSERT_EQ(expect.size(), dataSet.rows.size());
    for (size_t i = 0; i < expect.size(); i++) {
        const auto& expectRow = expect[i];
        const auto& actualRow = dataSet.rows[i];
        ASSERT_EQ(expectRow, actualRow);
    }
}

TEST(GetPropTest, PropertyTest) {
    fs::TempDir rootPath("/tmp/GetPropTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;

    {
        LOG(INFO) << "GetVertexPropInValue";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {kVid, "1.name", "1.age", "1.avgScore"};
        nebula::Row row({"Tim Duncan", "Tim Duncan", 44, 19.0});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.props_ref());
    }
    {
        LOG(INFO) << "GetVertexPropInKeyAndValue";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        tags.emplace_back(player, std::vector<std::string>{kVid, kTag, "name", "age", "avgScore"});
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {kVid, std::string("1.").append(kVid), std::string("1.").append(kTag),
                             "1.name", "1.age", "1.avgScore"};
        nebula::Row row({"Tim Duncan", "Tim Duncan", 1, "Tim Duncan", 44, 19.0});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.props_ref());
    }
    {
        LOG(INFO) << "MultiVertexGetProps";
        std::vector<VertexID> vertices = {"Tim Duncan", "Tony Parker"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {kVid, "1.name", "1.age", "1.avgScore"};
        {
            nebula::Row row({"Tony Parker", "Tony Parker", 38, 15.5});
            expected.rows.emplace_back(std::move(row));
        }
        {
            nebula::Row row({"Tim Duncan", "Tim Duncan", 44, 19.0});
            expected.rows.emplace_back(std::move(row));
        }
        ASSERT_EQ(expected, *resp.props_ref());
    }
    {
        LOG(INFO) << "MultiVertexGetMultiTagProps";
        std::vector<VertexID> vertices = {"Tim Duncan", "Tony Parker"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {kVid, "2.name", "1.name", "1.age", "1.avgScore"};
        {
            nebula::Row row({"Tony Parker", Value(), "Tony Parker", 38, 15.5});
            expected.rows.emplace_back(std::move(row));
        }
        {
            nebula::Row row({"Tim Duncan", Value(), "Tim Duncan", 44, 19.0});
            expected.rows.emplace_back(std::move(row));
        }
        ASSERT_EQ(expected, *resp.props_ref());
    }
    {
        LOG(INFO) << "MultiVertexGetMultiTagProps";
        std::vector<VertexID> vertices = {"Tim Duncan", "Spurs"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        tags.emplace_back(team, std::vector<std::string>{"name"});
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {kVid, "1.name", "1.age", "1.avgScore", "2.name"};
        {
            nebula::Row row({"Spurs", Value(), Value(), Value(), "Spurs"});
            expected.rows.emplace_back(std::move(row));
        }
        {
            nebula::Row row({"Tim Duncan", "Tim Duncan", 44, 19.0, Value()});
            expected.rows.emplace_back(std::move(row));
        }
        ASSERT_EQ(expected, *resp.props_ref());
    }
    {
        LOG(INFO) << "GetEdgePropInValue";
        std::vector<cpp2::EdgeKey> edgeKeys;
        {
            cpp2::EdgeKey edgeKey;
            edgeKey.set_src("Tim Duncan");
            edgeKey.set_edge_type(101);
            edgeKey.set_ranking(1997);
            edgeKey.set_dst("Spurs");
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        std::vector<std::pair<TagID, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = buildEdgeRequest(totalParts, edgeKeys, edges);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {"101.teamName", "101.startYear", "101.endYear"};
        nebula::Row row({"Spurs", 1997, 2016});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.props_ref());
    }
    {
        LOG(INFO) << "GetEdgePropInKeyAndValue";
        std::vector<cpp2::EdgeKey> edgeKeys;
        {
            cpp2::EdgeKey edgeKey;
            edgeKey.set_src("Tim Duncan");
            edgeKey.set_edge_type(101);
            edgeKey.set_ranking(1997);
            edgeKey.set_dst("Spurs");
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        std::vector<std::pair<TagID, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{
            kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
        auto req = buildEdgeRequest(totalParts, edgeKeys, edges);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {std::string("101.").append(kSrc), std::string("101.").append(kType),
                             std::string("101.").append(kRank), std::string("101.").append(kDst),
                             "101.teamName", "101.startYear", "101.endYear"};
        nebula::Row row({"Tim Duncan", 101, 1997, "Spurs", "Spurs", 1997, 2016});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.props_ref());
    }
    {
        LOG(INFO) << "GetEdgePropInValue";
        std::vector<cpp2::EdgeKey> edgeKeys;
        {
            cpp2::EdgeKey edgeKey;
            edgeKey.set_src("Tim Duncan");
            edgeKey.set_edge_type(101);
            edgeKey.set_ranking(1997);
            edgeKey.set_dst("Spurs");
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        {
            cpp2::EdgeKey edgeKey;
            edgeKey.set_src("Tony Parker");
            edgeKey.set_edge_type(101);
            edgeKey.set_ranking(2001);
            edgeKey.set_dst("Spurs");
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        std::vector<std::pair<TagID, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = buildEdgeRequest(totalParts, edgeKeys, edges);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {"101.teamName", "101.startYear", "101.endYear"};
        {
            nebula::Row row({"Spurs", 2001, 2018});
            expected.rows.emplace_back(std::move(row));
        }
        {
            nebula::Row row({"Spurs", 1997, 2016});
            expected.rows.emplace_back(std::move(row));
        }
        ASSERT_EQ(expected, *resp.props_ref());
    }
}

TEST(GetPropTest, AllPropertyInOneSchemaTest) {
    fs::TempDir rootPath("/tmp/GetPropTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    {
        LOG(INFO) << "GetVertexProp";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        tags.emplace_back(std::make_pair(player, std::vector<std::string>()));
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {kVid, "1.name", "1.age", "1.playing", "1.career",
                             "1.startYear", "1.endYear", "1.games", "1.avgScore",
                             "1.serveTeams", "1.country", "1.champions"};
        nebula::Row row({"Tim Duncan", "Tim Duncan", 44, false, 19,
                         1997, 2016, 1392, 19.0, 1, "America", 5});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.props_ref());
    }
    {
        LOG(INFO) << "GetEdgeProp";
        std::vector<cpp2::EdgeKey> edgeKeys;
        {
            cpp2::EdgeKey edgeKey;
            edgeKey.set_src("Tim Duncan");
            edgeKey.set_edge_type(101);
            edgeKey.set_ranking(1997);
            edgeKey.set_dst("Spurs");
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        std::vector<std::pair<TagID, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(serve, std::vector<std::string>()));
        auto req = buildEdgeRequest(totalParts, edgeKeys, edges);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        nebula::DataSet expected;
        expected.colNames = {"101.playerName", "101.teamName", "101.startYear", "101.endYear",
                             "101.teamCareer", "101.teamGames", "101.teamAvgScore", "101.type",
                             "101.champions"};
        nebula::Row row({"Tim Duncan", "Spurs", 1997, 2016, 19, 1392, 19.000000, "zzzzz", 5});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.props_ref());
    }
}

TEST(GetPropTest, AllPropertyInAllSchemaTest) {
    fs::TempDir rootPath("/tmp/GetPropTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    {
        LOG(INFO) << "GetVertexProp";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            nebula::Row row;
            // The first one is kVid, and player 11 properties in value
            std::vector<Value> values{"Tim Duncan", "Tim Duncan", 44, false, 19,
                                      1997, 2016, 1392, 19.0, 1, "America", 5};
            // team: 1 property, tag3: 11 property, in total 12
            for (size_t i = 0; i < 12; i++) {
                values.emplace_back(Value());
            }
            row.values = std::move(values);
            expected.emplace_back(std::move(row));
            // kVid, player: 11, team: 1, tag3: 11
            ASSERT_EQ(1 + 11 + 1 + 11, (*resp.props_ref()).colNames.size());
            verifyResult(expected, *resp.props_ref());
        }
    }
    {
        LOG(INFO) << "GetEdgeProp";
        std::vector<cpp2::EdgeKey> edgeKeys;
        {
            cpp2::EdgeKey edgeKey;
            edgeKey.set_src("Tim Duncan");
            edgeKey.set_edge_type(101);
            edgeKey.set_ranking(1997);
            edgeKey.set_dst("Spurs");
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        std::vector<std::pair<TagID, std::vector<std::string>>> edges;
        auto req = buildEdgeRequest(totalParts, edgeKeys, edges);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            nebula::Row row;
            std::vector<Value> values;
            // -teammate
            for (size_t i = 0; i < 5; i++) {
                values.emplace_back(Value());
            }
            // -serve
            for (size_t i = 0; i < 9; i++) {
                values.emplace_back(Value());
            }
            // serve
            values.emplace_back("Tim Duncan");
            values.emplace_back("Spurs");
            values.emplace_back(1997);
            values.emplace_back(2016);
            values.emplace_back(19);
            values.emplace_back(1392);
            values.emplace_back(19.0);
            values.emplace_back("zzzzz");
            values.emplace_back(5);
            // teammate
            for (size_t i = 0; i < 5; i++) {
                values.emplace_back(Value());
            }
            row.values = std::move(values);
            expected.emplace_back(std::move(row));
            ASSERT_TRUE(resp.props_ref().has_value());
            verifyResult(expected, *resp.props_ref());
        }
    }
    {
        LOG(INFO) << "GetNotExisted";
        std::vector<VertexID> vertices = {"Not existed"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            ASSERT_TRUE(resp.props_ref().has_value());
            verifyResult(expected, *resp.props_ref());
        }
    }
    {
        // Not exists and exists
        LOG(INFO) << "GetNotExisted";
        std::vector<VertexID> vertices = {"Not existed", "Tim Duncan"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            nebula::Row row;
            // The first one is kVid, and player have 11 properties in key and value
            std::vector<Value> values{"Tim Duncan", "Tim Duncan", 44, false, 19,
                                      1997, 2016, 1392, 19.0, 1, "America", 5};
            // team: 1 property, tag3: 11 property, in total 12
            for (size_t i = 0; i < 12; i++) {
                values.emplace_back(Value());
            }
            row.values = std::move(values);
            expected.emplace_back(std::move(row));
            // kVid, player: 11, team: 1, tag3: 11
            ASSERT_EQ(1 + 11 + 1 + 11, (*resp.props_ref()).colNames.size());
            verifyResult(expected, *resp.props_ref());
        }
    }
    {
        LOG(INFO) << "MultiKey";
        std::vector<VertexID> vertices = {"Tim Duncan", "Tony Parker"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            {
                nebula::Row row;
                // The first one is kVid, and player have 11 properties in key and value
                std::vector<Value> values{"Tony Parker", "Tony Parker", 38, false, 18,
                                          2001, 2019, 1254, 15.5, 2, "France", 4};
                // team: 1 property, tag3: 11 property, in total 12
                for (size_t i = 0; i < 12; i++) {
                    values.emplace_back(Value());
                }
                row.values = std::move(values);
                expected.emplace_back(std::move(row));
            }
            {
                nebula::Row row;
                // The first one is kVid, and player have 11 properties in key and value
                std::vector<Value> values{"Tim Duncan", "Tim Duncan", 44, false, 19,
                                          1997, 2016, 1392, 19.0, 1, "America", 5};
                // team: 1 property, tag3: 11 property, in total 12
                for (size_t i = 0; i < 12; i++) {
                    values.emplace_back(Value());
                }
                row.values = std::move(values);
                expected.emplace_back(std::move(row));
            }
            // kVid, player: 11, team: 1, tag3: 11
            ASSERT_EQ(1 + 11 + 1 + 11, (*resp.props_ref()).colNames.size());
            verifyResult(expected, *resp.props_ref());
        }
    }
}

TEST(GetPropTest, ConcurrencyTest) {
    FLAGS_query_concurrently = true;
    fs::TempDir rootPath("/tmp/GetPropTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    {
        LOG(INFO) << "MultiKey";
        std::vector<VertexID> vertices = {"Tim Duncan", "Tony Parker"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            {
                nebula::Row row;
                // The first one is kVid, and player have 11 properties in key and value
                std::vector<Value> values{"Tony Parker", "Tony Parker", 38, false, 18,
                                          2001, 2019, 1254, 15.5, 2, "France", 4};
                // team: 1 property, tag3: 11 property, in total 12
                for (size_t i = 0; i < 12; i++) {
                    values.emplace_back(Value());
                }
                row.values = std::move(values);
                expected.emplace_back(std::move(row));
            }
            {
                nebula::Row row;
                // The first one is kVid, and player have 11 properties in key and value
                std::vector<Value> values{"Tim Duncan", "Tim Duncan", 44, false, 19,
                                          1997, 2016, 1392, 19.0, 1, "America", 5};
                // team: 1 property, tag3: 11 property, in total 12
                for (size_t i = 0; i < 12; i++) {
                    values.emplace_back(Value());
                }
                row.values = std::move(values);
                expected.emplace_back(std::move(row));
            }
            // kVid, player: 11, team: 1, tag3: 11
            ASSERT_EQ(1 + 11 + 1 + 11, (*resp.props_ref()).colNames.size());
            verifyResult(expected, *resp.props_ref());
        }
    }
    FLAGS_query_concurrently = false;
}

TEST(QueryVertexPropsTest, PrefixBloomFilterTest) {
    FLAGS_enable_rocksdb_statistics = true;
    FLAGS_enable_rocksdb_prefix_filtering = true;

    fs::TempDir rootPath("/tmp/GetPropTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    GraphSpaceID spaceId = 1;
    TagID player = 1;
    EdgeType serve = 101;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    auto vIdLen = status.value();
    std::vector<VertexID> vertices = {"Tim Duncan", "Not Existed"};
    std::hash<std::string> hash;
    for (const auto& vId : vertices) {
        PartitionID partId = (hash(vId) % totalParts) + 1;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen, partId, vId, player);
        auto code = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
        ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
    }

    {
        std::shared_ptr<rocksdb::Statistics> statistics = kvstore::getDBStatistics();
        ASSERT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED), 0);
        ASSERT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
        ASSERT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
        statistics->Reset();
        ASSERT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED), 0);
        ASSERT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
        ASSERT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    }

    for (const auto& vId : vertices) {
        PartitionID partId = (hash(vId) % totalParts) + 1;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = NebulaKeyUtils::edgePrefix(vIdLen, partId, vId, serve);
        auto code = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
        ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
    }

    {
        std::shared_ptr<rocksdb::Statistics> statistics = kvstore::getDBStatistics();
        ASSERT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED), 0);
        ASSERT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
        ASSERT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    }

    FLAGS_enable_rocksdb_statistics = false;
    FLAGS_enable_rocksdb_prefix_filtering = false;
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
