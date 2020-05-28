/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

/*
TEST(GetNeighborsTest, PropertyTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        LOG(INFO) << "OneOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "OneOutEdgeKeyInProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
                                                           "_src", "_type", "_rank", "_dst"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "OneInEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "OneOutEdgeKeyInProperty";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer",
                           "_src", "_type", "_rank", "_dst"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "GetNotExistTag";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        // Ducan would have not add any data of tag team
        tags.emplace_back(team, std::vector<std::string>{"name"});
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "OutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{});
        edges.emplace_back(serve, std::vector<std::string>{});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "InEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{});
        edges.emplace_back(-serve, std::vector<std::string>{});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "Nullable";
        std::vector<VertexID> vertices = {"Steve Nash"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "champions"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "champions"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "DefaultValue";
        std::vector<VertexID> vertices = {"Dwight Howard"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "country"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "type"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "Misc";
        std::vector<VertexID> vertices = {"Dwight Howard"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{
            "name", "age", "country", "playing", "career", "startYear", "endYear", "games",
            "avgScore", "serveTeams", "country", "champions"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
            "teamCareer", "teamGames", "teamAvgScore", "type", "champions",
            "_src", "_type", "_rank", "_dst"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "MultiOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        LOG(INFO) << "InOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve, -teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});
        edges.emplace_back(-teammate, std::vector<std::string>{"player1", "player2", "teamName"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
}

TEST(GetNeighborsTest, GoFromMultiVerticesTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        LOG(INFO) << "OneOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan", "Tony Parker"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 2);
    }
    {
        LOG(INFO) << "OneInEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Spurs", "Rockets"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 2);
    }
    {
        LOG(INFO) << "Misc";
        std::vector<VertexID> vertices = {"Tracy McGrady", "Kobe Bryant"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{
            "name", "age", "country", "playing", "career", "startYear", "endYear", "games",
            "avgScore", "serveTeams", "country", "champions"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
            "teamCareer", "teamGames", "teamAvgScore", "type", "champions",
            "_src", "_type", "_rank", "_dst"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2",
            "teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 2);
    }
}

TEST(GetNeighborsTest, StatTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    {
        LOG(INFO) << "OneOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"LeBron James"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        std::vector<cpp2::StatProp> statProps;
        {
            // count champions in all served teams
            cpp2::StatProp statProp;
            statProp.type = serve;
            statProp.name = "champions";
            statProp.stat = cpp2::StatType::SUM;
            statProps.emplace_back(std::move(statProp));
        }
        {
            // avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.type = serve;
            statProp.name = "teamAvgScore";
            statProp.stat = cpp2::StatType::AVG;
            statProps.emplace_back(std::move(statProp));
        }
        req.stat_props = std::move(statProps);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        std::unordered_map<VertexID, std::vector<Value>> expectStat;
        expectStat.emplace("LeBron James", std::vector<Value>{3, (29.7 + 27.1 + 27.5 + 25.7) / 4});

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, &expectStat);
    }
}

TEST(GetNeighborsTest, SampleTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        FLAGS_max_edge_returned_per_vertex = 10;
        LOG(INFO) << "SingleEdgeTypeCutOff";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(1, resp.vertices.rows.size());
        // 4 column, vId, stat, team, -serve
        ASSERT_EQ(4, resp.vertices.rows[0].columns.size());
        ASSERT_EQ(10, resp.vertices.rows[0].columns[3].getDataSet().rows.size());
    }
    {
        FLAGS_max_edge_returned_per_vertex = 4;
        LOG(INFO) << "MultiEdgeTypeCutOff";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // 5 column, vId, stat, player, serve, teammate
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with cut off = 4, only serve edges will be returned
        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(1, resp.vertices.rows.size());
        ASSERT_EQ(5, resp.vertices.rows[0].columns.size());
        ASSERT_EQ(4, resp.vertices.rows[0].columns[3].getDataSet().rows.size());
        ASSERT_EQ(NullType::__NULL__, resp.vertices.rows[0].columns[4].getNull());
    }
    {
        FLAGS_max_edge_returned_per_vertex = 5;
        LOG(INFO) << "MultiEdgeTypeCutOff";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"playe1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // 5 column, vId, stat, player, serve, teammate
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with cut off = 5, 4 serve edges and 1 teammate edge will be returned
        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(1, resp.vertices.rows.size());
        ASSERT_EQ(5, resp.vertices.rows[0].columns.size());
        ASSERT_EQ(4, resp.vertices.rows[0].columns[3].getDataSet().rows.size());
        ASSERT_EQ(1, resp.vertices.rows[0].columns[4].getDataSet().rows.size());
    }
    FLAGS_max_edge_returned_per_vertex = INT_MAX;
}

TEST(GetNeighborsTest, VertexCacheTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    VertexCache vertexCache(1000, 4);

    TagID player = 1;
    EdgeType serve = 101;

    {
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, &vertexCache, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    {
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, &vertexCache, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
}

TEST(GetNeighborsTest, TtlTest) {
    FLAGS_mock_ttl_col = true;

    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    {
        LOG(INFO) << "OutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1);
    }
    sleep(FLAGS_mock_ttl_duration + 1);
    {
        LOG(INFO) << "OutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        ASSERT_EQ(1, resp.vertices.rows.size());
        ASSERT_EQ(4, resp.vertices.rows[0].columns.size());
        ASSERT_EQ(NullType::__NULL__, resp.vertices.rows[0].columns[2].getNull());
        ASSERT_EQ(NullType::__NULL__, resp.vertices.rows[0].columns[3].getNull());
    }
    FLAGS_mock_ttl_col = false;
}

TEST(GetNeighborsTest, FailedTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;
    EdgeType serve = 101;

    {
        LOG(INFO) << "TagNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(std::make_pair(9999, std::vector<std::string>{"name"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, resp.result.failed_parts.size());
        ASSERT_EQ(cpp2::ErrorCode::E_TAG_NOT_FOUND, resp.result.failed_parts.front().code);
    }
    {
        LOG(INFO) << "EdgeNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {9999};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(9999, std::vector<std::string>{"teamName"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, resp.result.failed_parts.size());
        ASSERT_EQ(cpp2::ErrorCode::E_EDGE_NOT_FOUND, resp.result.failed_parts.front().code);
    }
    {
        LOG(INFO) << "TagPropNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(std::make_pair(player, std::vector<std::string>{"prop_not_exists"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, resp.result.failed_parts.size());
        ASSERT_EQ(cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND, resp.result.failed_parts.front().code);
    }
    {
        LOG(INFO) << "EdgePropNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(serve, std::vector<std::string>{"prop_not_exists"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, resp.result.failed_parts.size());
        ASSERT_EQ(cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND, resp.result.failed_parts.front().code);
    }
}
*/

TEST(GetNeighborsTest, GoOverAllTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, +/- serve, +/- teammate, col_date
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 9);
    }
    {
        LOG(INFO) << "GoFromTeamOverAll";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, +/- serve, +/- teammate, col_date
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 9);
    }
    {
        LOG(INFO) << "GoFromPlayerOverInEdge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::IN_EDGE;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 7 column: vId, stat, player, team, -serve, -teammate, col_date
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 7);
    }
    {
        LOG(INFO) << "GoFromPlayerOverOutEdge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::OUT_EDGE;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 7 column: vId, stat, player, team, +serve, +teammate, col_date
        QueryTestUtils::checkResponse(resp.vertices, vertices, 1, 7);
    }
    {
        LOG(INFO) << "GoFromMultiPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan", "LeBron James", "Dwyane Wade"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, +/- serve, +/- teammate, col_date
        QueryTestUtils::checkResponse(resp.vertices, vertices, 3, 9);
    }
    {
        LOG(INFO) << "GoFromMultiTeamOverAll";
        std::vector<VertexID> vertices = {"Spurs", "Lakers", "Heat"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        req.edge_direction = cpp2::EdgeDirection::BOTH;

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        // 9 column: vId, stat, player, team, +/- serve, +/- teammate, col_date
        QueryTestUtils::checkResponse(resp.vertices, vertices, 3, 9);
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
