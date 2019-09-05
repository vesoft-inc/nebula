/* Copyright (c) 2019 vesoft inc. All rights reserved.
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

void verifyResult(nebula::cpp2::IndexType type,
                  std::vector<std::pair<std::string, std::string>> data) {
    const std::string kIndexPrefix = "_i";
    std::vector<std::string> vertices = {
            {"1_i100010"},
            {"2_i100011"},
            {"3_i100012"},
            {"4_i100013"},
            {"5_i100014"},
            {"6_i100015"},
            {"7_i100016"},
            {"8_i100017"},
            {"9_i100018"},
            {"10_i100019"},
    };
    std::vector<std::string> edges = {
            {"1_i200string_col_12string_col_13string_col_14101"},
            {"2_i200string_col_12string_col_13string_col_14101"},
            {"3_i200string_col_12string_col_13string_col_14101"},
            {"4_i200string_col_12string_col_13string_col_14101"},
            {"5_i200string_col_12string_col_13string_col_14101"},
            {"6_i200string_col_12string_col_13string_col_14101"},
            {"7_i200string_col_12string_col_13string_col_14101"},
            {"8_i200string_col_12string_col_13string_col_14101"},
            {"9_i200string_col_12string_col_13string_col_14101"},
            {"10_i200string_col_12string_col_13string_col_14101"}
    };

    std::vector<std::string> actual;

    switch (type) {
        case nebula::cpp2::IndexType::TAG :
        {
            for (const auto &row : data) {
                auto key = row.first;
                std::string raw;
                auto offset = sizeof(PartitionID) + kIndexPrefix.size() + sizeof(IndexID);
                raw = folly::to<std::string>(*reinterpret_cast<const PartitionID*>(key.c_str()))
                      + kIndexPrefix
                      + folly::to<std::string>(*reinterpret_cast<const IndexID *>
                                 (key.c_str() + sizeof(PartitionID) + kIndexPrefix.size()))
                      + key.substr(offset, (key.size() - offset - sizeof(VertexID)))
                      + folly::to<std::string>(*reinterpret_cast<const VertexID *>
                                 (key.c_str() + (key.size() - sizeof(VertexID))));
                actual.emplace_back(std::move(raw));
            }
            ASSERT_EQ(vertices, actual);
            break;
        }
        case nebula::cpp2::IndexType::EDGE :
        {
            for (const auto &row : data) {
                auto key = row.first;
                std::string raw;
                auto offset = sizeof(PartitionID) + kIndexPrefix.size() + sizeof(IndexID);
                raw = folly::to<std::string>(*reinterpret_cast<const PartitionID*>(key.c_str()))
                      + kIndexPrefix
                      + folly::to<std::string>(*reinterpret_cast<const IndexID *>
                (key.c_str() + sizeof(PartitionID) + kIndexPrefix.size()))
                      + key.substr(offset, (key.size() - offset - sizeof(EdgeType)))
                      + folly::to<std::string>(*reinterpret_cast<const EdgeType *>
                        (key.c_str() + (key.size() - sizeof(EdgeType))));
                actual.emplace_back(std::move(raw));
            }
            ASSERT_EQ(edges, actual);
            break;
        }
    }
}

TEST(IndexTest, IndexCreateTest) {
    FLAGS_load_data_interval_secs = 1;
    FLAGS_heartbeat_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    fs::TempDir rootPath("/tmp/IndexCreateTest.XXXXXX");
    GraphSpaceID spaceId = 0;
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    uint16_t localMetaPort = network::NetworkUtils::getAvailablePort();
    std::string metaPath = folly::stringPrintf("%s/meta", rootPath.path());
    auto metaServerContext = meta::TestUtils::mockMetaServer(localMetaPort,
                                                             metaPath.c_str(),
                                                             kClusterId);
    localMetaPort =  metaServerContext->port_;
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
    auto r = mClient->addHosts({HostAddr(localIp, localDataPort)}).get();
    ASSERT_TRUE(r.ok());
    mClient->waitForMetadReady();

    std::string dataPath = folly::stringPrintf("%s/data", rootPath.path());
    auto sc = TestUtils::mockStorageServer(mClient.get(),
                                           dataPath.c_str(),
                                           localIp,
                                           localDataPort);

    auto ret = mClient->createSpace("default", 10, 1).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    spaceId = ret.value();

    sleep(static_cast<unsigned int>(FLAGS_load_data_interval_secs + 1));
    auto* nKV = dynamic_cast<kvstore::NebulaStore*>(sc->kvStore_.get());
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
            break;
        }
        usleep(100000);
    }
    auto sClient = std::make_unique<StorageClient>(threadPool, mClient.get());

    // insert Vertices
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
        auto f = sClient->addVertices(spaceId, std::move(vertices), true);
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
    sleep(static_cast<unsigned int>(FLAGS_load_data_interval_secs + 1));
    {
        LOG(INFO) << "Begin create tag index...";
        std::map<int32_t, std::vector<std::string>> props;
        props.insert(std::pair<int32_t, std::vector<std::string>>(3001,
                                        {"tag_3001_col_0", "tag_3001_col_1"}));
        auto f = sClient->buildIndex(spaceId, nebula::cpp2::IndexType::TAG,  100, props);
        LOG(INFO) << "Waiting for the response...";
        auto resp = std::move(f).get();
        ASSERT_TRUE(resp.succeeded());
    }

    {
        std::vector<std::pair<std::string, std::string>> actual;

        for (auto partId = 1; partId <= 10; partId++) {
            std::unique_ptr<kvstore::KVIterator> iter;
            auto key = NebulaKeyUtils::indexPrefix(partId, 100);
            auto result = nKV->prefix(spaceId, partId, key, &iter);
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, result);

            while (iter->valid()) {
                auto ikey = iter->key().str();
                auto ival = iter->val().str();
                actual.emplace_back(std::pair<std::string, std::string>(
                                    ikey.substr(0, ikey.length() - sizeof(TagVersion)),
                                    ival.substr(0, ival.length() - sizeof(TagVersion))));
                iter->next();
            }
        }
        verifyResult(nebula::cpp2::IndexType::TAG, actual);
    }

    // insert edges
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
            edge.set_key(edgeKey);
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
        auto f = sClient->addEdges(spaceId, std::move(edges), true);
        LOG(INFO) << "Waiting for the response...";
        auto resp = std::move(f).get();
        ASSERT_TRUE(resp.succeeded());
    }

    {
        LOG(INFO) << "Begin create edge index...";
        std::map<int32_t, std::vector<std::string>> props;
        props.insert(std::pair<int32_t, std::vector<std::string>>(101,
                                        {"col_12", "col_13", "col_14"}));
        auto f = sClient->buildIndex(spaceId, nebula::cpp2::IndexType::EDGE, 200, props);
        LOG(INFO) << "Waiting for the response...";
        auto resp = std::move(f).get();
        ASSERT_TRUE(resp.succeeded());
    }

    {
        std::vector<std::pair<std::string, std::string>> actual;
        for (auto partId = 1; partId <= 10; partId++) {
            std::unique_ptr<kvstore::KVIterator> iter;
            auto key = NebulaKeyUtils::indexPrefix(partId, 200);
            auto result = nKV->prefix(spaceId, partId, key, &iter);
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, result);

            while (iter->valid()) {
                auto ikey = iter->key().str();
                auto ival = iter->val().str();
                actual.emplace_back(std::pair<std::string, std::string>(
                        ikey.substr(0, ikey.length() - sizeof(EdgeVersion)),
                        ival.substr(0, ival.length() - sizeof(EdgeVersion))));
                iter->next();
            }
        }
        verifyResult(nebula::cpp2::IndexType::EDGE, actual);
    }

    LOG(INFO) << "Stop meta client";
    mClient->stop();
    LOG(INFO) << "Stop data server...";
    sc.reset();
    LOG(INFO) << "Stop data client...";
    sClient.reset();
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


