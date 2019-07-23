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
    fs::TempDir rootPath("/tmp/StorageClientTest.XXXXXX");
    GraphSpaceID spaceId = 0;
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    // Let the system choose an available port for us
    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();
    LOG(INFO) << "Start meta server....";
    std::string metaPath = folly::stringPrintf("%s/meta", rootPath.path());
    auto metaServerContext = meta::TestUtils::mockMetaServer(localMetaPort, metaPath.c_str());
    localMetaPort =  metaServerContext->port_;

    LOG(INFO) << "Create meta client...";
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto addrsRet
        = network::NetworkUtils::toHosts(folly::stringPrintf("127.0.0.1:%d", localMetaPort));
    CHECK(addrsRet.ok()) << addrsRet.status();
    auto mClient
        = std::make_unique<meta::MetaClient>(threadPool, std::move(addrsRet.value()), true);
    mClient->init();

    LOG(INFO) << "Start data server....";

    // for mockStorageServer MetaServerBasedPartManager, use ephemeral port
    uint32_t localDataPort = network::NetworkUtils::getAvailablePort();
    std::string dataPath = folly::stringPrintf("%s/data", rootPath.path());
    auto sc = TestUtils::mockStorageServer(mClient.get(),
                                           dataPath.c_str(),
                                           localIp,
                                           localDataPort,
                                           // TODO We are using the memory version of
                                           // SchemaMan We need to switch to Meta Server
                                           // based version
                                           false);

    LOG(INFO) << "Add hosts and create space....";
    auto r = mClient->addHosts({HostAddr(localIp, localDataPort)}).get();
    ASSERT_TRUE(r.ok());
    while (meta::ActiveHostsMan::instance()->getActiveHosts().size() == 0) {
        usleep(1000);
    }
    VLOG(1) << "The storage server has been added to the meta service";

    auto ret = mClient->createSpace("default", 10, 1).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    spaceId = ret.value();
    LOG(INFO) << "Created space \"default\", its id is " << spaceId;
    sleep(FLAGS_load_data_interval_secs + 1);
    auto* nKV = static_cast<kvstore::NebulaStore*>(sc->kvStore_.get());
    while (true) {
        int readyNum = 0;
        for (auto partId = 1; partId <= 10; partId++) {
            auto retLeader = nKV->partLeader(spaceId, partId);
            if (ok(retLeader)) {
                auto leader = value(std::move(retLeader));
                if (leader != HostAddr(0, 0)) {
                    readyNum++;
                }
            }
        }
        if (readyNum == 10) {
            LOG(INFO) << "All leaders have been elected!";
            break;
        }
        usleep(100000);
    }
    auto client = std::make_unique<StorageClient>(threadPool, mClient.get());

    // VerticesInterfacesTest(addVertices and getVertexProps)
    {
        LOG(INFO) << "Prepare vertices data...";
        std::vector<storage::cpp2::Vertex> vertices;
        for (int32_t vId = 0; vId < 10; vId++) {
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
            retCols.emplace_back(
                TestUtils::propDef(cpp2::PropOwner::SOURCE,
                                   folly::stringPrintf("tag_%d_col_%d", 3001 + i*2, i*2),
                                   3001 + i*2));
        }
        auto f = client->getVertexProps(spaceId, std::move(vIds), std::move(retCols));
        auto resp = std::move(f).get();
        if (VLOG_IS_ON(2)) {
            if (!resp.succeeded()) {
                std::stringstream ss;
                for (auto& p : resp.failedParts()) {
                    ss << "Part " << p.first
                       << ": " << static_cast<int32_t>(p.second)
                       << "; ";
                }
                VLOG(2) << "Failed partitions:: " << ss.str();
            }
        }
        ASSERT_TRUE(resp.succeeded());

        auto& results = resp.responses();
        ASSERT_EQ(1, results.size());
        EXPECT_EQ(0, results[0].result.failed_codes.size());

        EXPECT_EQ(3, results[0].vertex_schema.columns.size());
        auto tagProvider = std::make_shared<ResultSchemaProvider>(results[0].vertex_schema);
        EXPECT_EQ(10, results[0].vertices.size());
        for (auto& vp : results[0].vertices) {
            auto tagReader = RowReader::getRowReader(vp.vertex_data, tagProvider);
            EXPECT_EQ(3, tagReader->numFields());
            int64_t col1;
            EXPECT_EQ(ResultType::SUCCEEDED, tagReader->getInt("tag_3001_col_0", col1));
            EXPECT_EQ(col1, 0);

            int64_t col2;
            EXPECT_EQ(ResultType::SUCCEEDED, tagReader->getInt("tag_3003_col_2", col2));
            EXPECT_EQ(col2, 2);

            folly::StringPiece col3;
            EXPECT_EQ(ResultType::SUCCEEDED, tagReader->getString("tag_3005_col_4", col3));
            EXPECT_EQ(folly::stringPrintf("tag_string_col_4"), col3);
        }
    }

    // EdgesInterfacesTest(addEdges and getEdgeProps)
    {
        LOG(INFO) << "Prepare edges data...";
        std::vector<storage::cpp2::Edge> edges;
        for (uint64_t srcId = 0; srcId < 10; srcId++) {
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
        for (uint64_t srcId = 0; srcId < 10; srcId++) {
            // Set the edge key.
            cpp2::EdgeKey edgeKey;
            edgeKey.set_src(srcId);
            edgeKey.set_edge_type(101);
            edgeKey.set_dst(srcId*100 + 2);
            edgeKey.set_ranking(srcId*100 + 3);
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        for (int i = 0; i < 20; i++) {
            retCols.emplace_back(
                TestUtils::propDef(cpp2::PropOwner::EDGE,
                                   folly::stringPrintf("col_%d", i)));
        }
        auto f = client->getEdgeProps(spaceId, std::move(edgeKeys), std::move(retCols));
        auto resp = std::move(f).get();
        ASSERT_TRUE(resp.succeeded());

        auto& results = resp.responses();
        ASSERT_EQ(1, results.size());
        EXPECT_EQ(0, results[0].result.failed_codes.size());
        EXPECT_EQ(3 + 20, results[0].schema.columns.size());

        auto edgeProvider = std::make_shared<ResultSchemaProvider>(results[0].schema);
        RowSetReader rsReader(edgeProvider, results[0].data);
        auto it = rsReader.begin();
        while (it) {
            EXPECT_EQ(3 + 20, it->numFields());
            auto fieldIt = it->begin();
            int index = 0;
            while (fieldIt) {
                if (index < 3) {  // _src | _rank | _dst
                    int64_t vid;
                    EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getVid(vid));
                } else if (index >= 13) {  // the last 10 STRING fields
                    folly::StringPiece stringCol;
                    EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getString(stringCol));
                    EXPECT_EQ(folly::stringPrintf("string_col_%d", index - 3), stringCol);
                } else {  // the middle 10 INT fields
                    int32_t intCol;
                    EXPECT_EQ(ResultType::SUCCEEDED, fieldIt->getInt(intCol));
                    EXPECT_EQ(index - 3, intCol);
                }
                ++index;
                ++fieldIt;
            }
            EXPECT_EQ(fieldIt, it->end());
            ++it;
        }
        EXPECT_EQ(it, rsReader.end());
    }
    LOG(INFO) << "Stop data server...";
    sc.reset();
    LOG(INFO) << "Stop data client...";
    client.reset();
    LOG(INFO) << "Stop meta server...";
    metaServerContext.reset();
    threadPool.reset();
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


