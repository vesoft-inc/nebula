/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "meta/test/TestUtils.h"
#include "storage/client/StorageClient.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "dataman/RowSetReader.h"
#include "network/NetworkUtils.h"

DECLARE_string(meta_server_addrs);
DECLARE_int32(load_data_interval_secs);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {

TEST(StorageClientTest, VerticesInterfacesTest) {
    FLAGS_load_data_interval_secs = 1;
    FLAGS_heartbeat_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    fs::TempDir rootPath("/tmp/StorageClientTest.XXXXXX");
    GraphSpaceID spaceId = 0;
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    // Let the system choose an available port for us
    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();
    LOG(INFO) << "Start meta server....";
    std::string metaPath = folly::stringPrintf("%s/meta", rootPath.path());
    auto metaServerContext = meta::TestUtils::mockMetaServer(localMetaPort,
                                                             metaPath.c_str(),
                                                             kClusterId);
    localMetaPort =  metaServerContext->port_;

    LOG(INFO) << "Create meta client...";
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto addrsRet
        = network::NetworkUtils::toHosts(folly::stringPrintf("127.0.0.1:%d", localMetaPort));
    CHECK(addrsRet.ok()) << addrsRet.status();
    auto& addrs = addrsRet.value();
    uint32_t localDataPort = network::NetworkUtils::getAvailablePort();
    auto hostRet = nebula::network::NetworkUtils::toHostAddr("127.0.0.1", localDataPort);
    auto& localHost = hostRet.value();
    auto mClient = std::make_unique<meta::MetaClient>(threadPool,
                                                      std::move(addrs),
                                                      localHost,
                                                      kClusterId,
                                                      true);
    LOG(INFO) << "Add hosts automatically and create space....";
    mClient->waitForMetadReady();
    VLOG(1) << "The storage server has been added to the meta service";

    LOG(INFO) << "Start data server....";

    // for mockStorageServer MetaServerBasedPartManager, use ephemeral port
    std::string dataPath = folly::stringPrintf("%s/data", rootPath.path());
    auto sc = TestUtils::mockStorageServer(mClient.get(),
                                           dataPath.c_str(),
                                           localIp,
                                           localDataPort,
                                           // TODO We are using the memory version of
                                           // SchemaMan We need to switch to Meta Server
                                           // based version
                                           false);

    auto ret = mClient->createSpace("default", 10, 1).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    spaceId = ret.value();
    LOG(INFO) << "Created space \"default\", its id is " << spaceId;
    sleep(FLAGS_load_data_interval_secs + 1);
    TestUtils::waitUntilAllElected(sc->kvStore_.get(), spaceId, 10);
    auto client = std::make_unique<StorageClient>(threadPool, mClient.get());

    // VerticesInterfacesTest(addVertices and getVertexProps)
    {
        LOG(INFO) << "Prepare vertices data...";
        std::vector<storage::cpp2::Vertex> vertices;
        for (int64_t vId = 0; vId < 10; vId++) {
            cpp2::Vertex v;
            v.set_id(vId);
            decltype(v.tags) tags;
            for (int32_t tagId = 3001; tagId < 3010; tagId++) {
                cpp2::Tag t;
                t.set_tag_id(tagId);
                // Generate some tag props.
                RowWriter writer;
                for (uint64_t numInt = 0; numInt < 3; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                t.set_props(writer.encode());
                tags.emplace_back(std::move(t));
            }
            v.set_tags(std::move(tags));
            vertices.emplace_back(std::move(v));
        }
        auto f = client->addVertices(spaceId, std::move(vertices), true);
        LOG(INFO) << "Waiting for the response...";
        auto resp = std::move(f).get();
        if (!resp.succeeded()) {
            for (auto& err : resp.failedParts()) {
                LOG(ERROR) << "Partition " << err.first
                           << " failed: " << static_cast<int32_t>(err.second);
            }
            ASSERT_TRUE(resp.succeeded());
        }
    }

    {
        std::vector<VertexID> vIds;
        std::vector<cpp2::PropDef> retCols;
        for (int32_t vId = 0; vId < 10; vId++) {
            vIds.emplace_back(vId);
        }
        for (int i = 0; i < 3; i++) {
            retCols.emplace_back(TestUtils::vertexPropDef(
                folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2), 3001 + i * 2));
        }
        auto f = client->getVertexProps(spaceId, std::move(vIds), std::move(retCols));
        auto resp = std::move(f).get();
        if (VLOG_IS_ON(2)) {
            if (!resp.succeeded()) {
                std::stringstream ss;
                for (auto& p : resp.failedParts()) {
                    ss << "Part " << p.first << ": " << static_cast<int32_t>(p.second) << "; ";
                }
                VLOG(2) << "Failed partitions:: " << ss.str();
            }
        }
        ASSERT_TRUE(resp.succeeded());

        auto& results = resp.responses();
        ASSERT_EQ(1, results.size());
        EXPECT_EQ(0, results[0].result.failed_codes.size());

        EXPECT_EQ(10, results[0].vertices.size());

        auto* vschema = results[0].get_vertex_schema();
        DCHECK(vschema != nullptr);
        for (auto& vp : results[0].vertices) {
            auto size = std::accumulate(vp.tag_data.cbegin(), vp.tag_data.cend(), 0,
                                        [vschema](int acc, auto& td) {
                                            auto it = vschema->find(td.tag_id);
                                            DCHECK(it != vschema->end());
                                            return acc + it->second.columns.size();
                                        });

            EXPECT_EQ(3, size);

            checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", vschema, 0);
            checkTagData<int64_t>(vp.tag_data, 3003, "tag_3003_col_2", vschema, 2);
            checkTagData<std::string>(vp.tag_data, 3005, "tag_3005_col_4", vschema,
                                      folly::stringPrintf("tag_string_col_4"));
        }
    }

    // EdgesInterfacesTest(addEdges and getEdgeProps)
    {
        LOG(INFO) << "Prepare edges data...";
        std::vector<storage::cpp2::Edge> edges;
        for (int64_t srcId = 0; srcId < 10; srcId++) {
            cpp2::Edge edge;
            // Set the edge key.
            decltype(edge.key) edgeKey;
            edgeKey.set_src(srcId);
            edgeKey.set_edge_type(101);
            edgeKey.set_dst(srcId*100 + 2);
            edgeKey.set_ranking(srcId*100 + 3);
            edge.set_key(std::move(edgeKey));
            // Generate some edge props.
            RowWriter writer;
            for (int32_t iInt = 0; iInt < 10; iInt++) {
                writer << iInt;
            }
            for (int32_t iString = 10; iString < 20; iString++) {
                writer << folly::stringPrintf("string_col_%d", iString);
            }
            edge.set_props(writer.encode());
            edges.emplace_back(std::move(edge));
        }
        auto f = client->addEdges(spaceId, std::move(edges), true);
        LOG(INFO) << "Waiting for the response...";
        auto resp = std::move(f).get();
        ASSERT_TRUE(resp.succeeded());
    }

    {
        std::vector<storage::cpp2::EdgeKey> edgeKeys;
        std::vector<cpp2::PropDef> retCols;
        for (int64_t srcId = 0; srcId < 10; srcId++) {
            // Set the edge key.
            cpp2::EdgeKey edgeKey;
            edgeKey.set_src(srcId);
            edgeKey.set_edge_type(101);
            edgeKey.set_dst(srcId*100 + 2);
            edgeKey.set_ranking(srcId*100 + 3);
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        for (int i = 0; i < 20; i++) {
            retCols.emplace_back(TestUtils::edgePropDef(folly::stringPrintf("col_%d", i), 101));
        }
        auto f = client->getEdgeProps(spaceId, std::move(edgeKeys), std::move(retCols));
        auto resp = std::move(f).get();
        ASSERT_TRUE(resp.succeeded());

        auto& results = resp.responses();
        ASSERT_EQ(1, results.size());
        EXPECT_EQ(0, results[0].result.failed_codes.size());
        EXPECT_EQ(4 + 20, results[0].schema.columns.size());

        auto edgeProvider = std::make_shared<ResultSchemaProvider>(results[0].schema);
        RowSetReader rsReader(edgeProvider, results[0].data);
        auto it = rsReader.begin();
        while (it) {
            EXPECT_EQ(4 + 20, it->numFields());
            auto fieldIt = it->begin();
            int index = 0;
            while (fieldIt) {
                if (index < 4) {  // _src | _rank | _dst
                    int64_t vid;
                    EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getVid(vid));
                } else if (index >= 14) {  // the last 10 STRING fields
                    folly::StringPiece stringCol;
                    EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getString(stringCol));
                    EXPECT_EQ(folly::stringPrintf("string_col_%d", index - 4), stringCol);
                } else {  // the middle 10 INT fields
                    int32_t intCol;
                    EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getInt(intCol));
                    EXPECT_EQ(index - 4, intCol);
                }
                ++index;
                ++fieldIt;
            }
            EXPECT_EQ(fieldIt, it->end());
            ++it;
        }
        EXPECT_EQ(it, rsReader.end());
    }
    {
        for (int64_t srcId = 0; srcId < 10; srcId++) {
            std::vector<cpp2::EdgeKey> edgeKeys;
            // Get all edgeKeys of a vertex
            {
                auto f = client->getEdgeKeys(spaceId, srcId);
                auto resp = std::move(f).get();
                ASSERT_TRUE(resp.ok());
                auto edgeKeyResp =  std::move(resp).value();
                auto& result = edgeKeyResp.get_result();
                ASSERT_EQ(0, result.get_failed_codes().size());
                edgeKeys = *(edgeKeyResp.get_edge_keys());

                // Check edgeKeys
                CHECK_EQ(1, edgeKeys.size());
                auto& edge = edgeKeys[0];
                CHECK_EQ(srcId, edge.get_src());
                CHECK_EQ(101, edge.get_edge_type());
                CHECK_EQ(srcId*100 + 3, edge.get_ranking());
                CHECK_EQ(srcId*100 + 2, edge.get_dst());
            }
            // Delete all edges of a vertex
            {
                auto f = client->deleteEdges(spaceId, edgeKeys);
                auto resp = std::move(f).get();
                ASSERT_TRUE(resp.succeeded());

                // Check that edges have been successfully deleted
                auto cf = client->getEdgeKeys(spaceId, srcId);
                auto cresp = std::move(cf).get();
                ASSERT_TRUE(cresp.ok());
                auto edgeKeyResp =  std::move(cresp).value();
                auto& result = edgeKeyResp.get_result();
                ASSERT_EQ(0, result.get_failed_codes().size());
                ASSERT_EQ(0, edgeKeyResp.get_edge_keys()->size());
            }
            // Delete a vertex
            {
                auto f = client->deleteVertex(spaceId, srcId);
                auto resp = std::move(f).get();
                ASSERT_TRUE(resp.ok());
                auto  execResp = std::move(resp).value();
                auto& result = execResp.get_result();
                ASSERT_EQ(0, result.get_failed_codes().size());

                // Check that this vertex has been successfully deleted
                std::vector<VertexID> vIds{srcId};
                std::vector<cpp2::PropDef> retCols;
                retCols.emplace_back(
                    TestUtils::vertexPropDef(folly::stringPrintf("tag_%d_col_%d", 3001, 0), 3001));
                auto cf = client->getVertexProps(spaceId, std::move(vIds), std::move(retCols));
                auto cresp = std::move(cf).get();
                ASSERT_TRUE(cresp.succeeded());
                auto& results = cresp.responses();
                ASSERT_EQ(1, results.size());
                EXPECT_EQ(0, results[0].result.failed_codes.size());
                // TODO bug: the results[0].vertices.size should be equal 0
                EXPECT_EQ(1, results[0].vertices.size());
                EXPECT_EQ(0, results[0].vertices[0].tag_data.size());
            }
        }
    }

    {
        // get not existed uuid
        std::vector<VertexID> vIds;
        for (int i = 0; i < 10; i++) {
            auto status = client->getUUID(spaceId, std::to_string(i)).get();
            ASSERT_TRUE(status.ok());
            auto resp = status.value();
            vIds.emplace_back(resp.get_id());
        }

        for (int i = 0; i < 10; i++) {
            auto status = client->getUUID(spaceId, std::to_string(i)).get();
            ASSERT_TRUE(status.ok());
            auto resp = status.value();
            ASSERT_EQ(resp.get_id(), vIds[i]);
        }
    }
    LOG(INFO) << "Stop meta client";
    mClient->stop();
    LOG(INFO) << "Stop data server...";
    sc.reset();
    LOG(INFO) << "Stop data client...";
    client.reset();
    LOG(INFO) << "Stop meta server...";
    metaServerContext.reset();
    threadPool.reset();
}

#define RETURN_LEADER_CHANGED(req, leader) \
    UNUSED(req); \
    do { \
        folly::Promise<storage::cpp2::QueryResponse> pro; \
        auto f = pro.getFuture(); \
        storage::cpp2::QueryResponse resp; \
        storage::cpp2::ResponseCommon rc; \
        rc.failed_codes.emplace_back(); \
        auto& code = rc.failed_codes.back(); \
        code.set_part_id(1); \
        code.set_code(storage::cpp2::ErrorCode::E_LEADER_CHANGED); \
        code.set_leader(leader); \
        resp.set_result(std::move(rc)); \
        pro.setValue(std::move(resp)); \
        return f; \
    } while (false)

class TestStorageServiceRetry : public storage::cpp2::StorageServiceSvIf {
public:
    TestStorageServiceRetry(IPv4 ip, Port port) {
        leader_.set_ip(ip);
        leader_.set_port(port);
    }

    folly::Future<cpp2::QueryResponse>
    future_getBound(const cpp2::GetNeighborsRequest& req) override {
        RETURN_LEADER_CHANGED(req, leader_);
    }

private:
    nebula::cpp2::HostAddr leader_;
};

class TestStorageClient : public StorageClient {
public:
    explicit TestStorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool)
        : StorageClient(ioThreadPool, nullptr) {}

    StatusOr<int32_t> partsNum(GraphSpaceID) const override {
        return parts_.size();
    }

    StatusOr<PartMeta> getPartMeta(GraphSpaceID, PartitionID partId) const override {
        auto it = parts_.find(partId);
        CHECK(it != parts_.end());
        return it->second;
    }

    std::unordered_map<PartitionID, PartMeta> parts_;
};

TEST(StorageClientTest, LeaderChangeTest) {
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto sc = std::make_unique<test::ServerContext>();
    auto handler = std::make_shared<TestStorageServiceRetry>(localIp, 10010);
    sc->mockCommon("storage", 0, handler);
    LOG(INFO) << "Start storage server on " << sc->port_;

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    TestStorageClient tsc(threadPool);
    PartMeta pm;
    pm.spaceId_ = 1;
    pm.partId_ = 1;
    pm.peers_.emplace_back(HostAddr(localIp, sc->port_));
    tsc.parts_.emplace(1, std::move(pm));

    folly::Baton<true, std::atomic> baton;
    tsc.getNeighbors(1, {1, 2, 3}, {0}, "", {}).via(threadPool.get()).thenValue([&] (auto&&) {
        baton.post();
    });
    baton.wait();
    ASSERT_EQ(1, tsc.leaders_.size());
    ASSERT_EQ(HostAddr(localIp, 10010), tsc.leaders_[std::make_pair(1, 1)]);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
