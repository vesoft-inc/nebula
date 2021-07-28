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
ObjectPool objPool;
auto pool = &objPool;

TEST(GetNeighborsTest, PropertyTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "OneOutEdgeKeyInProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore", kVid, kTag});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
                                                           kSrc, kType, kRank, kDst});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, team, - serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "OneInEdgeKeyInProperty";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name", kVid, kTag});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer",
                           kSrc, kType, kRank, kDst});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, team, - serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "GetNotExistTag";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        // Duncan would have not add any data of tag team
        tags.emplace_back(team, std::vector<std::string>{"name"});
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, team, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 6);
    }
    {
        LOG(INFO) << "OneOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{});
        edges.emplace_back(serve, std::vector<std::string>{});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "OutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::OUT_EDGE);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 6);
    }
    {
        LOG(INFO) << "InEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::IN_EDGE);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, - teammate, - serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 6);
    }
    {
        LOG(INFO) << "InOutEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, - teammate, - serve, serve, teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 8);
    }
    {
        LOG(INFO) << "InEdgeReturnAllProperty";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, - teammate, - serve, serve, teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 8);
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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "Misc";
        std::vector<VertexID> vertices = {"Dwight Howard"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{
            "name", "age", "playing", "career", "startYear", "endYear", "games",
            "avgScore", "serveTeams", "country", "champions", kVid, kTag});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
            "playerName", "teamCareer", "teamGames", "teamAvgScore", "type", "champions",
            kSrc, kType, kRank, kDst});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 6);
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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, teammate, -teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 7);
    }
}

TEST(GetNeighborsTest, GoFromMultiVerticesTest) {
    FLAGS_query_concurrently = true;
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 2, 5);
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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, -serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 2, 5);
    }
    {
        LOG(INFO) << "OneOutEdgeMultiProperty";
        std::vector<VertexID> vertices = {"Tim Duncan",
                                          "Tony Parker",
                                          "LaMarcus Aldridge",
                                          "Rudy Gay",
                                          "Marco Belinelli",
                                          "Danny Green",
                                          "Kyle Anderson",
                                          "Aron Baynes",
                                          "Boris Diaw",
                                          "Tiago Splitter"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 10, 5);
    }
    {
        LOG(INFO) << "Misc";
        std::vector<VertexID> vertices = {"Tracy McGrady", "Kobe Bryant"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{
            "name", "age", "playing", "career", "startYear", "endYear", "games",
            "avgScore", "serveTeams", "country", "champions"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear",
            "teamCareer", "teamGames", "teamAvgScore", "type", "champions",
            kSrc, kType, kRank, kDst});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2",
            "teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 2, 6);
    }
    FLAGS_query_concurrently = false;
}

TEST(GetNeighborsTest, StatTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    TagID player = 1;
    EdgeType serve = 101;

    {
        LOG(INFO) << "CollectStatOfDifferentProperty";
        std::vector<VertexID> vertices = {"LeBron James"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        std::vector<cpp2::StatProp> statProps;
        {
            // count teamGames_ in all served history
            cpp2::StatProp statProp;
            statProp.set_alias("Total games");
            const auto& exp =
                *EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamGames");
            statProp.set_prop(Expression::encode(exp));
            statProp.set_stat(cpp2::StatType::SUM);
            statProps.emplace_back(std::move(statProp));
        }
        {
            // avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.set_alias("Avg scores in all served teams");
            const auto& exp =
                *EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamAvgScore");
            statProp.set_prop(Expression::encode(exp));
            statProp.set_stat(cpp2::StatType::AVG);
            statProps.emplace_back(std::move(statProp));
        }
        {
            // longest consecutive team career in a team
            cpp2::StatProp statProp;
            statProp.set_alias("Longest consecutive team career in a team");
            const auto& exp =
                *EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamCareer");
            statProp.set_prop(Expression::encode(exp));
            statProp.set_stat(cpp2::StatType::MAX);
            statProps.emplace_back(std::move(statProp));
        }
        {
            // sum of rank in serve edge
            cpp2::StatProp statProp;
            statProp.set_alias("Sum of rank in serve edge");
            const auto& exp = *EdgeRankExpression::make(pool, folly::to<std::string>(serve));
            statProp.set_prop(Expression::encode(exp));
            statProp.set_stat(cpp2::StatType::SUM);
            statProps.emplace_back(std::move(statProp));
        }
        (*req.traverse_spec_ref()).set_stat_props(std::move(statProps));

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        std::unordered_map<VertexID, std::vector<Value>> expectStat;
        expectStat.emplace("LeBron James",
                           std::vector<Value>{548 + 294 + 301 + 115,
                                              (29.7 + 27.1 + 27.5 + 25.7) / 4,
                                              7,
                                              2003 + 2010 + 2014 + 2018});

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(
            *resp.vertices_ref(), vertices, over, tags, edges, 1, 5, &expectStat);
    }
    {
        LOG(INFO) << "CollectStatOfSameProperty";
        std::vector<VertexID> vertices = {"LeBron James"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve,
                           std::vector<std::string>{"teamName", "startYear", "teamAvgScore"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        std::vector<cpp2::StatProp> statProps;
        {
            // avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.set_alias("Avg scores in all served teams");
            const auto& exp =
                *EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamAvgScore");
            statProp.set_prop(Expression::encode(exp));
            statProp.set_stat(cpp2::StatType::AVG);
            statProps.emplace_back(std::move(statProp));
        }
        {
            // min avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.set_alias("Min scores in all served teams");
            const auto& exp =
                *EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamAvgScore");
            statProp.set_prop(Expression::encode(exp));
            statProp.set_stat(cpp2::StatType::MIN);
            statProps.emplace_back(std::move(statProp));
        }
        {
            // max avg scores in all served teams
            cpp2::StatProp statProp;
            statProp.set_alias("Max scores in all served teams");
            const auto& exp =
                *EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamAvgScore");
            statProp.set_prop(Expression::encode(exp));
            statProp.set_stat(cpp2::StatType::MAX);
            statProps.emplace_back(std::move(statProp));
        }
        (*req.traverse_spec_ref()).set_stat_props(std::move(statProps));

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        std::unordered_map<VertexID, std::vector<Value>> expectStat;
        expectStat.emplace("LeBron James",
                           std::vector<Value>{(29.7 + 27.1 + 27.5 + 25.7) / 4, 25.7, 29.7});

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(
            *resp.vertices_ref(), vertices, over, tags, edges, 1, 5, &expectStat);
    }
}

TEST(GetNeighborsTest, LimitSampleTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        LOG(INFO) << "SingleEdgeTypeLimit";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_limit(10);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        // vId, stat, team, -serve, expr
        ASSERT_EQ(5, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ(10, (*resp.vertices_ref()).rows[0].values[3].getList().values.size());
    }
    {
        LOG(INFO) << "MultiEdgeTypeLimit";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_limit(4);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // vId, stat, player, serve, teammate, expr
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with limit = 4, return all serve edges, none of the teammate edges will be returned
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        ASSERT_EQ(6, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ(4, (*resp.vertices_ref()).rows[0].values[3].getList().values.size());
        ASSERT_EQ(Value::Type::__EMPTY__, (*resp.vertices_ref()).rows[0].values[4].type());
    }
    {
        LOG(INFO) << "SingleEdgeTypeSample";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_limit(10);
        (*req.traverse_spec_ref()).set_random(true);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        // vId, stat, team, -serve, expr
        ASSERT_EQ(5, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ(10, (*resp.vertices_ref()).rows[0].values[3].getList().values.size());
    }
    {
        LOG(INFO) << "MultiEdgeTypeSample";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_limit(4);
        (*req.traverse_spec_ref()).set_random(true);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // vId, stat, player, serve, teammate, expr
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with sample = 4
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        ASSERT_EQ(6, (*resp.vertices_ref()).rows[0].values.size());
        size_t actual = 0;
        if ((*resp.vertices_ref()).rows[0].values[3].type() == Value::Type::LIST) {
            actual += (*resp.vertices_ref()).rows[0].values[3].getList().values.size();
        }
        if ((*resp.vertices_ref()).rows[0].values[4].type() == Value::Type::LIST) {
            actual += (*resp.vertices_ref()).rows[0].values[4].getList().values.size();
        }
        ASSERT_EQ(4, actual);
    }
    {
        LOG(INFO) << "MultiEdgeTypeSample";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_limit(5);
        (*req.traverse_spec_ref()).set_random(true);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // 5 column, vId, stat, player, serve, teammate
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with sample = 5
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        ASSERT_EQ(6, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ(5, (*resp.vertices_ref()).rows[0].values[3].getList().values.size() +
                     (*resp.vertices_ref()).rows[0].values[4].getList().values.size());
    }
}

TEST(GetNeighborsTest, MaxEdgReturnedPerVertexTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    auto defaultVal = FLAGS_max_edge_returned_per_vertex;
    FLAGS_max_edge_returned_per_vertex = 10;
    {
        LOG(INFO) << "SingleEdgeTypeLimit";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        // vId, stat, team, -serve, expr
        ASSERT_EQ(5, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ(10, (*resp.vertices_ref()).rows[0].values[3].getList().values.size());
    }

    FLAGS_max_edge_returned_per_vertex = 4;
    {
        LOG(INFO) << "MultiEdgeTypeLimit";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // vId, stat, player, serve, teammate, expr
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with limit = 4, return all serve edges, none of the teammate edges will be returned
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        ASSERT_EQ(6, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ(4, (*resp.vertices_ref()).rows[0].values[3].getList().values.size());
        ASSERT_EQ(Value::Type::__EMPTY__, (*resp.vertices_ref()).rows[0].values[4].type());
    }

    FLAGS_max_edge_returned_per_vertex = 10;
    {
        LOG(INFO) << "SingleEdgeTypeSample";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{
                           "playerName", "startYear", "teamCareer"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_random(true);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        // vId, stat, team, -serve, expr
        ASSERT_EQ(5, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ(10, (*resp.vertices_ref()).rows[0].values[3].getList().values.size());
    }

    FLAGS_max_edge_returned_per_vertex = 4;
    {
        LOG(INFO) << "MultiEdgeTypeSample";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_random(true);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // vId, stat, player, serve, teammate, expr
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with sample = 4
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        ASSERT_EQ(6, (*resp.vertices_ref()).rows[0].values.size());
        size_t actual = 0;
        if ((*resp.vertices_ref()).rows[0].values[3].type() == Value::Type::LIST) {
            actual += (*resp.vertices_ref()).rows[0].values[3].getList().values.size();
        }
        if ((*resp.vertices_ref()).rows[0].values[4].type() == Value::Type::LIST) {
            actual += (*resp.vertices_ref()).rows[0].values[4].getList().values.size();
        }
        ASSERT_EQ(4, actual);
    }

    FLAGS_max_edge_returned_per_vertex = 5;
    {
        LOG(INFO) << "MultiEdgeTypeSample";
        std::vector<VertexID> vertices = {"Dwyane Wade"};
        std::vector<EdgeType> over = {serve, teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});

        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_random(true);
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        // 5 column, vId, stat, player, serve, teammate
        // Dwyane Wad has 4 serve edge, 2 teammate edge
        // with sample = 5
        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        ASSERT_EQ(6, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ(5, (*resp.vertices_ref()).rows[0].values[3].getList().values.size() +
                     (*resp.vertices_ref()).rows[0].values[4].getList().values.size());
    }

    FLAGS_max_edge_returned_per_vertex = defaultVal;
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
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 5);
    }
    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 10);
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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        // vId, stat, player, serve, expr
        ASSERT_EQ(5, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ("Tim Duncan", (*resp.vertices_ref()).rows[0].values[0].getStr());
        ASSERT_EQ(Value::Type::__EMPTY__, (*resp.vertices_ref()).rows[0].values[1].type());
        ASSERT_EQ(Value::Type::__EMPTY__, (*resp.vertices_ref()).rows[0].values[2].type());
        ASSERT_EQ(Value::Type::__EMPTY__, (*resp.vertices_ref()).rows[0].values[3].type());
        ASSERT_EQ(Value::Type::__EMPTY__, (*resp.vertices_ref()).rows[0].values[4].type());
    }
    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate, expr
        ASSERT_EQ(1, (*resp.vertices_ref()).rows.size());
        ASSERT_EQ(10, (*resp.vertices_ref()).rows[0].values.size());
        ASSERT_EQ("Tim Duncan", (*resp.vertices_ref()).rows[0].values[0].getStr());
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[1].empty());   // stat
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[2].empty());   // player expired
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[3].empty());   // team not exists
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[4].empty());   // general tag not exists
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[5].isList());  // - teammate valid
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[6].empty());   // - serve expired
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[7].empty());   // + serve expired
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[8].isList());  // + teammate valid
        ASSERT_TRUE((*resp.vertices_ref()).rows[0].values[9].empty());   // expr
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
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

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

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND,
                  (*resp.result_ref()).failed_parts.front().code);
    }
    {
        LOG(INFO) << "EdgeNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {9999};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(9999, std::vector<std::string>{"teamName"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND,
                (*resp.result_ref()).failed_parts.front().code);
    }
    {
        LOG(INFO) << "TagPropNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(std::make_pair(player, std::vector<std::string>{"prop_not_exists"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND,
                (*resp.result_ref()).failed_parts.front().code);
    }
    {
        LOG(INFO) << "EdgePropNotExists";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(serve, std::vector<std::string>{"prop_not_exists"}));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(1, (*resp.result_ref()).failed_parts.size());
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND,
                (*resp.result_ref()).failed_parts.front().code);
    }
}

TEST(GetNeighborsTest, ReturnAllPropertyTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        LOG(INFO) << "ReturnAllPropertyInTagAndEdge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        tags.emplace_back(std::make_pair(player, std::vector<std::string>()));
        tags.emplace_back(std::make_pair(team, std::vector<std::string>()));
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(serve, std::vector<std::string>()));
        edges.emplace_back(std::make_pair(-teammate, std::vector<std::string>()));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, serve, teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 7);
    }
    {
        LOG(INFO) << "ReturnAllPropertyInTagAndEdge";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        tags.emplace_back(std::make_pair(player, std::vector<std::string>()));
        tags.emplace_back(std::make_pair(team, std::vector<std::string>()));
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(std::make_pair(-serve, std::vector<std::string>()));
        edges.emplace_back(std::make_pair(teammate, std::vector<std::string>()));
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, -serve, teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 7);
    }
}

TEST(GetNeighborsTest, GoOverAllTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    {
        LOG(INFO) << "NoPropertyReturned";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges, true);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 3);
    }
    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 10);
    }
    {
        LOG(INFO) << "GoFromTeamOverAll";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 10);
    }
    {
        LOG(INFO) << "GoFromPlayerOverInEdge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::IN_EDGE);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, - serve, - teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 8);
    }
    {
        LOG(INFO) << "GoFromPlayerOverOutEdge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::OUT_EDGE);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, + serve, + teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 8);
    }
    {
        LOG(INFO) << "GoFromMultiPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan", "LeBron James", "Dwyane Wade"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 3, 10);
    }
    {
        LOG(INFO) << "GoFromMultiTeamOverAll";
        std::vector<VertexID> vertices = {"Spurs", "Lakers", "Heat"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 3, 10);
    }
}

TEST(GetNeighborsTest, MultiVersionTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts, 3));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    {
        LOG(INFO) << "GoFromPlayerOverAll";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
        (*req.traverse_spec_ref()).set_edge_direction(cpp2::EdgeDirection::BOTH);

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, team, general tag, - teammate, - serve, + serve, + teammate, expr
        QueryTestUtils::checkResponse(*resp.vertices_ref(), vertices, over, tags, edges, 1, 10);
    }
}

TEST(GetNeighborsTest, FilterTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    TagID player = 1;
    TagID team = 2;
    EdgeType serve = 101;
    EdgeType teammate = 102;

    {
        LOG(INFO) << "RelExp";
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamAvgScore > 20
            const auto& exp = *RelationalExpression::makeGT(
                pool,
                EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamAvgScore"),
                ConstantExpression::make(pool, Value(20)));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        nebula::DataSet expected;
        expected.colNames = {kVid,
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:+101:teamName:startYear:endYear",
                             "_expr"};
        nebula::Row row({"Tracy McGrady",
                         Value(),
                         nebula::List({"Tracy McGrady", 41, 19.6}),
                         nebula::List({nebula::List({"Magic", 2000, 2004}),
                                       nebula::List({"Rockets", 2004, 2010})}),
                         Value()});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.vertices_ref());
    }
    {
        LOG(INFO) << "ArithExpression";
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.endYear - serve.startYear > 5
            const auto& exp = *RelationalExpression::makeGT(
                pool,
                ArithmeticExpression::makeMinus(
                    pool,
                    EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "endYear"),
                    EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "startYear")),
                ConstantExpression::make(pool, Value(5)));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        nebula::DataSet expected;
        expected.colNames = {kVid,
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:+101:teamName:startYear:endYear",
                             "_expr"};
        auto serveEdges = nebula::List();
        serveEdges.values.emplace_back(nebula::List({"Rockets", 2004, 2010}));
        nebula::Row row({"Tracy McGrady",
                         Value(),
                         nebula::List({"Tracy McGrady", 41, 19.6}),
                         serveEdges,
                         Value()});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.vertices_ref());
    }
    {
        LOG(INFO) << "LogicalExp";
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamAvgScore > 20 && serve.teamCareer <= 4
            const auto& exp = *LogicalExpression::makeAnd(
                pool,
                RelationalExpression::makeGT(
                    pool,
                    EdgePropertyExpression::make(
                        pool, folly::to<std::string>(serve), "teamAvgScore"),
                    ConstantExpression::make(pool, Value(20))),
                RelationalExpression::makeLE(
                    pool,
                    EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamCareer"),
                    ConstantExpression::make(pool, Value(4))));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        nebula::DataSet expected;
        expected.colNames = {kVid,
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:+101:teamName:startYear:endYear",
                             "_expr"};
        auto serveEdges = nebula::List();
        serveEdges.values.emplace_back(nebula::List({"Magic", 2000, 2004}));
        nebula::Row row({"Tracy McGrady",
                         Value(),
                         nebula::List({"Tracy McGrady", 41, 19.6}),
                         serveEdges,
                         Value()});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.vertices_ref());
    }
    {
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve._dst == Rockets
            const auto& exp = *RelationalExpression::makeEQ(
                pool,
                EdgeDstIdExpression::make(pool, folly::to<std::string>(serve)),
                ConstantExpression::make(pool, Value("Rockets")));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        nebula::DataSet expected;
        expected.colNames = {kVid,
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:+101:teamName:startYear:endYear",
                             "_expr"};
        auto serveEdges = nebula::List();
        serveEdges.values.emplace_back(nebula::List({"Rockets", 2004, 2010}));
        nebula::Row row({"Tracy McGrady",
                         Value(),
                         nebula::List({"Tracy McGrady", 41, 19.6}),
                         serveEdges,
                         Value()});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.vertices_ref());
    }
    {
        LOG(INFO) << "Tag + Edge exp";
        std::vector<VertexID> vertices = {"Tracy McGrady"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamAvgScore > 20 && $^.player.games < 1000
            const auto& exp = *LogicalExpression::makeAnd(
                pool,
                RelationalExpression::makeGT(
                    pool,
                    EdgePropertyExpression::make(
                        pool, folly::to<std::string>(serve), "teamAvgScore"),
                    ConstantExpression::make(pool, Value(20))),
                RelationalExpression::makeLE(
                    pool,
                    SourcePropertyExpression::make(pool, folly::to<std::string>(player), "games"),
                    ConstantExpression::make(pool, Value(1000))));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        nebula::DataSet expected;
        expected.colNames = {kVid,
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:+101:teamName:startYear:endYear",
                             "_expr"};
        nebula::Row row({"Tracy McGrady",
                         Value(),
                         nebula::List({"Tracy McGrady", 41, 19.6}),
                         nebula::List({nebula::List({"Magic", 2000, 2004}),
                                       nebula::List({"Rockets", 2004, 2010})}),
                         Value()});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.vertices_ref());
    }
    {
        LOG(INFO) << "Filter apply to multi vertices";
        std::vector<VertexID> vertices = {"Tracy McGrady", "Tim Duncan", "Tony Parker",
                                          "Manu Ginobili"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamAvgScore > 18 && $^.player.avgScore > 18
            const auto& exp = *LogicalExpression::makeAnd(
                pool,
                RelationalExpression::makeGT(
                    pool,
                    EdgePropertyExpression::make(
                        pool, folly::to<std::string>(serve), "teamAvgScore"),
                    ConstantExpression::make(pool, Value(18))),
                RelationalExpression::makeGT(pool,
                                             SourcePropertyExpression::make(
                                                 pool, folly::to<std::string>(player), "avgScore"),
                                             ConstantExpression::make(pool, Value(18))));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        nebula::DataSet expected;
        expected.colNames = {kVid,
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:+101:teamName:startYear:endYear",
                             "_expr"};
        ASSERT_EQ(expected.colNames, (*resp.vertices_ref()).colNames);
        ASSERT_EQ(4, (*resp.vertices_ref()).rows.size());
        {
            nebula::Row row({"Tracy McGrady",
                            Value(),
                            nebula::List({"Tracy McGrady", 41, 19.6}),
                            nebula::List({nebula::List({"Magic", 2000, 2004}),
                                          nebula::List({"Rockets", 2004, 2010})}),
                            Value()});
            for (size_t i = 0; i < 4; i++) {
                if ((*resp.vertices_ref()).rows[i].values[0].getStr() == "Tracy McGrady") {
                    ASSERT_EQ(row, (*resp.vertices_ref()).rows[i]);
                    break;
                }
            }
        }
        {
            auto serveEdges = nebula::List();
            serveEdges.values.emplace_back(nebula::List({"Spurs", 1997, 2016}));
            nebula::Row row({"Tim Duncan",
                            Value(),
                            nebula::List({"Tim Duncan", 44, 19.0}),
                            serveEdges,
                            Value()});
            for (size_t i = 0; i < 4; i++) {
                if ((*resp.vertices_ref()).rows[i].values[0].getStr() == "Tim Duncan") {
                    ASSERT_EQ(row, (*resp.vertices_ref()).rows[i]);
                    break;
                }
            }
        }
        {
            // same as 1.0, tag data is returned even if can't pass the filter.
            // no edge satisfies the filter
            nebula::Row row({"Tony Parker",
                            Value(),
                            nebula::List({"Tony Parker", 38, 15.5}),
                            Value(),
                            Value()});
            for (size_t i = 0; i < 4; i++) {
                if ((*resp.vertices_ref()).rows[i].values[0].getStr() == "Tony Parker") {
                    ASSERT_EQ(row, (*resp.vertices_ref()).rows[i]);
                    break;
                }
            }
        }
        {
            // same as 1.0, tag data is returned even if can't pass the filter.
            // no edge satisfies the filter
            nebula::Row row({"Manu Ginobili",
                            Value(),
                            nebula::List({"Manu Ginobili", 42, 13.3}),
                            Value(),
                            Value()});
            for (size_t i = 0; i < 4; i++) {
                if ((*resp.vertices_ref()).rows[i].values[0].getStr() == "Manu Ginobili") {
                    ASSERT_EQ(row, (*resp.vertices_ref()).rows[i]);
                    break;
                }
            }
        }
    }
    {
        LOG(INFO) << "Go over multi edges, but filter is only applied to one edge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(teammate, std::vector<std::string>{"player1", "player2", "teamName"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // where serve.teamGames > 1000
            const auto& exp = *RelationalExpression::makeGT(
                pool,
                EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamGames"),
                ConstantExpression::make(pool, Value(1000)));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        nebula::DataSet expected;
        expected.colNames = {kVid,
                             "_stats",
                             "_tag:1:name:age:avgScore",
                             "_edge:+101:teamName:startYear:endYear",
                             "_edge:+102:player1:player2:teamName",
                             "_expr"};
        auto serveEdges = nebula::List();
        serveEdges.values.emplace_back(nebula::List({"Spurs", 1997, 2016}));
        // This will only get the edge of serve, which does not make sense
        // see https://github.com/vesoft-inc/nebula/issues/2166
        nebula::Row row({"Tim Duncan",
                         Value(),
                         nebula::List({"Tim Duncan", 44, 19.0}),
                         serveEdges,
                         Value(),
                         Value()});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.vertices_ref());
    }
    {
        LOG(INFO) << "FilterOnReverseEdge";
        std::vector<VertexID> vertices = {"Spurs"};
        std::vector<EdgeType> over = {-serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(team, std::vector<std::string>{"name"});
        edges.emplace_back(-serve, std::vector<std::string>{"teamName", "startYear", "endYear",
                                                            kSrc, kDst});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // The edgeName in exp will be unique, we don't distinguish it from reverse edges.
            // where reverseServe.teamAvgScore >= 10
            const auto& exp = *RelationalExpression::makeGE(
                pool,
                EdgePropertyExpression::make(pool, folly::to<std::string>(serve), "teamAvgScore"),
                ConstantExpression::make(pool, Value(15)));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }

        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, serve, expr
        nebula::DataSet expected;
        expected.colNames = {
            kVid,
            "_stats",
            "_tag:2:name",
            folly::stringPrintf("_edge:-101:teamName:startYear:endYear:%s:%s", kSrc, kDst),
            "_expr"};
        nebula::Row row({
            "Spurs",
            Value(),
            nebula::List({"Spurs"}),
            nebula::List({nebula::List({"Spurs", 1997, 2016, "Spurs", "Tim Duncan"}),
                            nebula::List({"Spurs", 2001, 2018, "Spurs", "Tony Parker"}),
                            nebula::List({"Spurs", 2015, 2020, "Spurs", "LaMarcus Aldridge"})}),
            Value()});
        expected.rows.emplace_back(std::move(row));
        ASSERT_EQ(expected, *resp.vertices_ref());
    }
    {
        LOG(INFO) << "FilterOnBidirectEdge";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {teammate, -teammate};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(
            teammate,
            std::vector<std::string>{kSrc, kType, kDst, "teamName", "startYear", "endYear"});
        edges.emplace_back(
            -teammate,
            std::vector<std::string>{kSrc, kType, kDst, "teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);

        {
            // The edgeName in exp will be unique, we don't distinguish it from reverse edges.
            // where teammate.startYear < 2002
            const auto& exp = *RelationalExpression::makeLT(
                pool,
                EdgePropertyExpression::make(pool, folly::to<std::string>(teammate), "startYear"),
                ConstantExpression::make(pool, Value(2002)));
            (*req.traverse_spec_ref()).set_filter(Expression::encode(exp));
        }
        auto* processor = GetNeighborsProcessor::instance(env, nullptr, threadPool.get());
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, (*resp.result_ref()).failed_parts.size());
        // vId, stat, player, teammate, -teammate, expr
        nebula::DataSet expected;
        expected.colNames = {
            kVid,
            "_stats",
            "_tag:1:name:age:avgScore",
            folly::stringPrintf("_edge:+102:%s:%s:%s:teamName:startYear:endYear",
                                kSrc, kType, kDst),
            folly::stringPrintf("_edge:-102:%s:%s:%s:teamName:startYear:endYear",
                                kSrc, kType, kDst),
            "_expr"};
        nebula::Row row({
            "Tim Duncan",
            Value(),
            nebula::List({"Tim Duncan", 44, 19.0}),
            nebula::List(nebula::List({"Tim Duncan", 102, "Tony Parker", "Spurs", 2001, 2016})),
            nebula::List(nebula::List({"Tim Duncan", -102, "Tony Parker", "Spurs", 2001, 2016})),
            Value(),
            Value()});
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
