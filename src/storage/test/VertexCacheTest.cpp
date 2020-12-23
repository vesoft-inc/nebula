/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include "storage/test/TestUtils.h"
#include "storage/test/QueryTestUtils.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "common/expression/ConstantExpression.h"
#include "storage/query/GetPropProcessor.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "codec/RowReader.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace storage {

cpp2::GetPropRequest
buildVertexRequest(int32_t totalParts,
                   const std::vector<VertexID>& vertices,
                   const std::vector<std::pair<TagID, std::vector<std::string>>>& tags) {
    std::hash<std::string> hash;
    cpp2::GetPropRequest req;
    req.space_id = 1;

    for (const auto& vertex : vertices) {
        PartitionID partId = (hash(vertex) % totalParts) + 1;
        nebula::Row row;
        row.values.emplace_back(vertex);
        req.parts[partId].emplace_back(std::move(row));
    }

    std::vector<cpp2::VertexProp> vertexProps;
    if (tags.empty()) {
        req.set_vertex_props(std::move(vertexProps));
    } else {
        for (const auto& tag : tags) {
            TagID tagId = tag.first;
            cpp2::VertexProp tagProp;
            tagProp.tag = tagId;
            for (const auto& prop : tag.second) {
                tagProp.props.emplace_back(std::move(prop));
            }
            vertexProps.emplace_back(std::move(tagProp));
        }
        req.set_vertex_props(std::move(vertexProps));
    }
    return req;
}

// Used to get player tag data
void getVertices(storage::StorageEnv* env,
                 int totalParts,
                 VertexCache* cache,
                 TagID tagId,
                 std::vector<VertexID>& vertices,
                 int expectNum) {
    LOG(INFO) << "GetVertexProp";
    std::vector<std::pair<TagID, std::vector<std::string>>> tags;
    tags.emplace_back(tagId, std::vector<std::string>{"name"});

    auto req = buildVertexRequest(totalParts, vertices, tags);

    auto* processor = GetPropProcessor::instance(env, nullptr, cache);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();

    ASSERT_EQ(0, resp.result.failed_parts.size());
    ASSERT_EQ(expectNum, resp.props.rows.size());
}

void checkCache(VertexCache* cache, uint64_t evicts, uint64_t hits, uint64_t total) {
    EXPECT_EQ(evicts, cache->evicts());
    EXPECT_EQ(hits, cache->hits());
    EXPECT_EQ(total, cache->total());
}


// In vertexCache, when the data of vertex is queried for the first time,
// the data is put in lru.
// When writing vertex data, if the data is in the vertexCache,
// the data will be evicted from the vertexCache.
// Operation data, add vertex, update vertex, delete vertex,
// not fetch vertex data(getVertexProp, getNeighbors, Lookup)
TEST(VertexCacheTest, OperationVertexTest) {
    fs::TempDir rootPath("/tmp/VertexCacheTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    // Here ConcurrentLRUCache capacity is the size of the player data(51), and
    // has a bucket.
    VertexCache cache(51, 0);

    // Add vertices
    // VertexCache is empty, add vertice only evicts vertex from VertexCache
    {
        auto* processor = AddVerticesProcessor::instance(env, nullptr, &cache);
        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
        LOG(INFO) << "Test AddVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        LOG(INFO) << "Check data in kv store...";
        // The number of data in players and teams is 81
        checkAddVerticesData(req, env, 81, 0);
    }

    // check vertexCache
    checkCache(&cache, 0, 0, 0);

    // Update vertex, not filter, update success
    // VertexCache is empty, update vertex evicts vertex from VertexCache first.
    // then get vertex data from VertexCache in tagNode. So total is 1.
    // TODO can be optimized here
    {
        GraphSpaceID spaceId = 1;
        TagID tagId = 1;
        auto status = env->schemaMan_->getSpaceVidLen(spaceId);
        ASSERT_TRUE(status.ok());
        auto spaceVidLen = status.value();

        LOG(INFO) << "Build UpdateVertexRequest...";
        cpp2::UpdateVertexRequest req;

        req.set_space_id(spaceId);
        auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
        VertexID vertexId("Tim Duncan");
        req.set_part_id(partId);
        req.set_vertex_id(vertexId);
        req.set_tag_id(tagId);

        LOG(INFO) << "Build updated props...";
        std::vector<cpp2::UpdatedProp> updatedProps;
        // int: player.age = 45
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("age");
        ConstantExpression val1(45L);
        uProp1.set_value(Expression::encode(val1));
        updatedProps.emplace_back(uProp1);

        // string: player.country= China
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("country");
        std::string col4new("China");
        ConstantExpression val2(col4new);
        uProp2.set_value(Expression::encode(val2));
        updatedProps.emplace_back(uProp2);
        req.set_updated_props(std::move(updatedProps));

        LOG(INFO) << "Build yield...";
        // Return player props: name, age, country
        decltype(req.return_props) tmpProps;
        auto* yTag1 = new std::string("1");
        auto* yProp1 = new std::string("name");
        SourcePropertyExpression sourcePropExp1(yTag1, yProp1);
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        auto* yTag2 = new std::string("1");
        auto* yProp2 = new std::string("age");
        SourcePropertyExpression sourcePropExp2(yTag2, yProp2);
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        auto* yTag3 = new std::string("1");
        auto* yProp3 = new std::string("country");
        SourcePropertyExpression sourcePropExp3(yTag3, yProp3);
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        auto* yTag4 = new std::string("1");
        auto* yProp4 = new std::string(kVid);
        SourcePropertyExpression sourcePropExp4(yTag4, yProp4);
        tmpProps.emplace_back(Expression::encode(sourcePropExp4));

        auto* yTag5 = new std::string("1");
        auto* yProp5 = new std::string(kTag);
        SourcePropertyExpression sourcePropExp5(yTag5, yProp5);
        tmpProps.emplace_back(Expression::encode(sourcePropExp5));

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(false);

        LOG(INFO) << "Test UpdateVertexRequest...";
        auto* processor = UpdateVertexProcessor::instance(env, nullptr, &cache);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_parts.size());
        EXPECT_EQ(6, resp.props.colNames.size());
        EXPECT_EQ("_inserted", resp.props.colNames[0]);
        EXPECT_EQ("1.name", resp.props.colNames[1]);
        EXPECT_EQ("1.age", resp.props.colNames[2]);
        EXPECT_EQ("1.country", resp.props.colNames[3]);
        EXPECT_EQ(std::string("1.").append(kVid), resp.props.colNames[4]);
        EXPECT_EQ(std::string("1.").append(kTag), resp.props.colNames[5]);

        EXPECT_EQ(1, resp.props.rows.size());
        EXPECT_EQ(6, resp.props.rows[0].values.size());

        EXPECT_EQ(false, resp.props.rows[0].values[0].getBool());
        EXPECT_EQ("Tim Duncan", resp.props.rows[0].values[1].getStr());
        EXPECT_EQ(45, resp.props.rows[0].values[2].getInt());
        EXPECT_EQ("China", resp.props.rows[0].values[3].getStr());
        EXPECT_EQ("Tim Duncan", resp.props.rows[0].values[4].getStr());
        EXPECT_EQ(1, resp.props.rows[0].values[5].getInt());

        // get player from kvstore directly
        auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        EXPECT_TRUE(iter && iter->valid());

        auto reader =
            RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
        auto val = reader->getValueByName("name");
        EXPECT_EQ("Tim Duncan", val.getStr());
        val = reader->getValueByName("age");
        EXPECT_EQ(45, val.getInt());
        val = reader->getValueByName("country");
        EXPECT_EQ("China", val.getStr());
    }

    // check vertexCache
    checkCache(&cache, 0, 0, 1);

    // Delete vertices
    // VertexCache is empty, delete vertice only evicts vertex from VertexCache.
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr, &cache);
        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();
        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        LOG(INFO) << "Check data in kv store...";
        auto ret = env->schemaMan_->getSpaceVidLen(req.space_id);
        EXPECT_TRUE(ret.ok());
        auto spaceVidLen = ret.value();
        // All the added datas are deleted, the number of vertices is 0
        checkVerticesData(spaceVidLen, req.space_id, req.parts, env, 0);
    }

    // check vertexCache
    checkCache(&cache, 0, 0, 1);
}


// Operation data, add vertex, update vertex, delete vertex,
// getNeighbors (getVertexProp, getNeighbors, Lookup)
TEST(VertexCacheTest, GetNeighborsTest) {
    fs::TempDir rootPath("/tmp/VertexCacheTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    // Here ConcurrentLRUCache capacity is the size of the player data(51), and
    // has a bucket.
    VertexCache cache(51, 0);

    // Add vertices
    // VertexCache is empty, add vertice only evicts vertex from VertexCache.
    {
        auto* processor = AddVerticesProcessor::instance(env, nullptr, &cache);
        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
        LOG(INFO) << "Test AddVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        LOG(INFO) << "Check data in kv store...";
        // The number of data in players and teams is 81
        checkAddVerticesData(req, env, 81, 0);
    }

    // Check vertexCache
    checkCache(&cache, 0, 0, 0);

    // Get neighbors
    {
        LOG(INFO) << "Get neighbors...";
        // players tagId
        TagID player = 1;
        EdgeType serve = 101;
        // When the get vertex prop is executed for the first time,
        // the vertexCache is empty at first, and exactly 51 records are
        // placed in the vertexCache. evicts is 0, hits is 0, total is 51.
        ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, parts));

        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<EdgeType> over = {serve};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        tags.emplace_back(player, std::vector<std::string>{"name", "age", "avgScore"});
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        auto req = QueryTestUtils::buildRequest(parts, vertices, over, tags, edges);

        // When the get neighbor is executed for the first time,
        // the vertexCache is empty at first, and the record is
        // placed in the vertexCache. evicts is 0, hits is 0, total is 1.
        {
            auto* processor = GetNeighborsProcessor::instance(env, nullptr, &cache);
            auto fut = processor->getFuture();
            processor->process(req);
            auto resp = std::move(fut).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            // vId, stat, player, serve, expr
            QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
        }
        checkCache(&cache, 0, 0, 1);

        // When the get neighbor is executed for the second time,
        // evicts is 0, hits is 1, total is 2.
        {
            auto* processor = GetNeighborsProcessor::instance(env, nullptr, &cache);
            auto fut = processor->getFuture();
            processor->process(req);
            auto resp = std::move(fut).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            // vId, stat, player, serve, expr
            QueryTestUtils::checkResponse(resp.vertices, vertices, over, tags, edges, 1, 5);
        }
        checkCache(&cache, 0, 1, 2);
    }

    // Delete vertices
    // If there is data in vertexCache, delete vertice only evicts vertex from VertexCache.
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr, &cache);
        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();
        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        LOG(INFO) << "Check data in kv store...";
        auto ret = env->schemaMan_->getSpaceVidLen(req.space_id);
        EXPECT_TRUE(ret.ok());
        auto spaceVidLen = ret.value();
        // All the added datas are deleted, the number of vertices is 0
        checkVerticesData(spaceVidLen, req.space_id, req.parts, env, 0);
    }
    // check vertexCache
    checkCache(&cache, 1, 1, 2);
}


// Operation data, add vertex, update vertex, delete vertex,
// fetch vertex data(getVertexProp, getNeighbors, Lookup)
TEST(VertexCacheTest, GetVertexPropTest) {
    fs::TempDir rootPath("/tmp/VertexCacheTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    // Here ConcurrentLRUCache capacity is the size of the player data(51), and
    // has a bucket.
    VertexCache cache(51, 0);

    // Add vertices
    // VertexCache is empty, add vertice only evicts vertex from VertexCache.
    {
        auto* processor = AddVerticesProcessor::instance(env, nullptr, &cache);
        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
        LOG(INFO) << "Test AddVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        LOG(INFO) << "Check data in kv store...";
        // The number of data in players and teams is 81
        checkAddVerticesData(req, env, 81, 0);
    }

    // Check vertexCache
    checkCache(&cache, 0, 0, 0);

    // Get vertex prop
    {
        LOG(INFO) << "Get players tag vertices...";
        // players tagId
        TagID tagId = 1;
        // players vertices
        auto vertices = mock::MockData::mockPlayerVerticeIds();

        // When the get vertex prop is executed for the first time,
        // the vertexCache is empty at first, and exactly 51 records are
        // placed in the vertexCache. evicts is 0, hits is 0, total is 51.
        getVertices(env, parts, &cache, tagId, vertices, 51);
        checkCache(&cache, 0, 0, 51);


        // When the get vertex prop is executed for the second time,
        // evicts is 0, hits is 51, total is 102.
        getVertices(env, parts, &cache, tagId, vertices, 51);
        checkCache(&cache, 0, 51, 102);
    }

    // Delete vertices
    // If there is data in vertexCache, delete vertice only evicts vertex from VertexCache.
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr, &cache);
        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();
        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        LOG(INFO) << "Check data in kv store...";
        auto ret = env->schemaMan_->getSpaceVidLen(req.space_id);
        EXPECT_TRUE(ret.ok());
        auto spaceVidLen = ret.value();
        // All the added datas are deleted, the number of vertices is 0
        checkVerticesData(spaceVidLen, req.space_id, req.parts, env, 0);
    }
     // check vertexCache
    checkCache(&cache, 51, 51, 102);
}


// Operation data, add vertex, update vertex, delete vertex,
// fetch vertex data(getVertexProp, getNeighbors, Lookup)
TEST(VertexCacheTest, GetVertexPropWithTTLTest) {
    FLAGS_mock_ttl_col = true;
    FLAGS_mock_ttl_duration = 5;

    fs::TempDir rootPath("/tmp/VertexCacheTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    // Here ConcurrentLRUCache capacity is the size of the player data(51), and
    // has a bucket.
    VertexCache cache(51, 0);

    // Add vertices
    // VertexCache is empty, add vertice only evicts vertex from VertexCache.
    {
        auto* processor = AddVerticesProcessor::instance(env, nullptr, &cache);
        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
        LOG(INFO) << "Test AddVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        LOG(INFO) << "Check data in kv store...";
        // The number of data in players and teams is 81
        checkAddVerticesData(req, env, 81, 0);
    }

    // Check vertexCache
    checkCache(&cache, 0, 0, 0);

    // Get vertex prop
    {
        LOG(INFO) << "Get players vertices...";
        // players tagId
        TagID tagId = 1;
        // players vertices
        auto vertices = mock::MockData::mockPlayerVerticeIds();

        // When the get vertex prop is executed for the first time,
        // the vertexCache is empty at first, and exactly 51 records are
        // placed in the vertexCache. evicts is 0, hits is 0, total is 51.
        getVertices(env, parts, &cache, tagId, vertices, 51);
        checkCache(&cache, 0, 0, 51);

        // Wait ttl data Expire
        sleep(FLAGS_mock_ttl_duration + 1);

        // When the get vertex prop is executed for the second time,
        // data expired because of ttl, delete it from vertexCache.
        // hits is 51, total is 102, evicts 51.
        // The total amount of get data is 0.

        // TODO At present, when the ttl data expires, tag returns a vid,
        // other attributes are empty value, edge returns a row with an empty value fields.
        getVertices(env, parts, &cache, tagId, vertices, 0);
        checkCache(&cache, 51, 51, 102);
    }

    // Delete vertices
    // If there is data in vertexCache, delete it from vertexCache.
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr, &cache);
        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();
        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        LOG(INFO) << "Check data in kv store...";
        auto ret = env->schemaMan_->getSpaceVidLen(req.space_id);
        EXPECT_TRUE(ret.ok());
        auto spaceVidLen = ret.value();
        // All the added datas are deleted, the number of vertices is 0
        checkVerticesData(spaceVidLen, req.space_id, req.parts, env, 0);
    }

    // check vertexCache
    checkCache(&cache, 51, 51, 102);

    FLAGS_mock_ttl_col = false;
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

