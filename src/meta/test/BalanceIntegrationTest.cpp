/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>
#include "meta/processors/admin/Balancer.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/client/StorageClient.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(raft_heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

TEST(BalanceIntegrationTest, BalanceTest) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_raft_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/balance_integration_test.XXXXXX");
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    const nebula::ClusterID kClusterId = 10;

    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();
    LOG(INFO) << "Start meta server....";
    std::string metaPath = folly::stringPrintf("%s/meta", rootPath.path());
    auto metaServerContext = meta::TestUtils::mockMetaServer(localMetaPort, metaPath.c_str(),
                                                             kClusterId);
    localMetaPort = metaServerContext->port_;

    auto adminClient = std::make_unique<AdminClient>(metaServerContext->kvStore_.get());
    Balancer balancer(metaServerContext->kvStore_.get(), std::move(adminClient));

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(10);
    std::vector<HostAddr> metaAddr = {HostAddr(localIp, localMetaPort)};

    LOG(INFO) << "Create meta client...";
    auto mClient = std::make_unique<meta::MetaClient>(threadPool,
                                                      metaAddr);

    mClient->waitForMetadReady();

    int partition = 1;
    int replica = 3;
    LOG(INFO) << "Start " << replica << " storage services, partition number " << partition;
    std::vector<HostAddr> peers;
    std::vector<uint32_t> storagePorts;
    std::vector<std::shared_ptr<meta::MetaClient>> metaClients;
    std::vector<std::unique_ptr<test::ServerContext>> serverContexts;
    for (int i = 0; i < replica; i++) {
        uint32_t storagePort = network::NetworkUtils::getAvailablePort();
        HostAddr storageAddr(localIp, storagePort);
        storagePorts.emplace_back(storagePort);
        peers.emplace_back(storageAddr);

        VLOG(1) << "The storage server has been added to the meta service";

        meta::MetaClientOptions options;
        options.localHost_ = storageAddr;
        options.clusterId_ = kClusterId;
        options.role_ = meta::cpp2::HostRole::STORAGE;
        auto metaClient = std::make_shared<meta::MetaClient>(threadPool,
                                                             metaAddr,
                                                             options);
        metaClient->waitForMetadReady();
        metaClients.emplace_back(metaClient);
    }

    for (int i = 0; i < replica; i++) {
        std::string dataPath = folly::stringPrintf("%s/%d/data", rootPath.path(), i);
        auto sc = storage::TestUtils::mockStorageServer(metaClients[i].get(),
                                                        dataPath.c_str(),
                                                        localIp,
                                                        storagePorts[i],
                                                        true);
        serverContexts.emplace_back(std::move(sc));
    }

    LOG(INFO) << "Create space and schema";
    SpaceDesc spaceDesc("storage", partition, replica);
    auto ret = mClient->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok());
    auto spaceId = ret.value();

    std::vector<cpp2::ColumnDef> columns;
    nebula::cpp2::ValueType vt;
    vt.set_type(SupportedType::STRING);
    columns.emplace_back();
    columns.back().set_name("c");
    columns.back().set_type(vt);

    cpp2::Schema schema;
    schema.set_columns(std::move(columns));
    auto tagRet = mClient->createTagSchema(spaceId, "tag", std::move(schema)).get();
    ASSERT_TRUE(tagRet.ok());
    auto tagId = tagRet.value();
    sleep(FLAGS_heartbeat_interval_secs + FLAGS_raft_heartbeat_interval_secs + 3);

    LOG(INFO) << "Let's write some data";
    auto sClient = std::make_unique<storage::StorageClient>(threadPool, mClient.get());
    {
        std::vector<storage::cpp2::Vertex> vertices;
        for (int32_t vId = 0; vId < 10000; vId++) {
            storage::cpp2::Vertex v;
            v.set_id(vId);
            decltype(v.tags) tags;
            storage::cpp2::Tag t;
            t.set_tag_id(tagId);
            // Generate some tag props.
            RowWriter writer;
            writer << std::string(1024, 'A');
            t.set_props(writer.encode());
            tags.emplace_back(std::move(t));
            v.set_tags(std::move(tags));
            vertices.emplace_back(std::move(v));
        }
        int retry = 10;
        while (retry-- > 0) {
            auto f = sClient->addVertices(spaceId, vertices, true);
            LOG(INFO) << "Waiting for the response...";
            auto resp = std::move(f).get();
            if (resp.completeness() == 100) {
                LOG(INFO) << "All requests has been processed!";
                break;
            }
            if (!resp.succeeded()) {
                for (auto& err : resp.failedParts()) {
                    LOG(ERROR) << "Partition " << err.first
                               << " failed: " << apache::thrift::util::enumNameSafe(err.second);
                }
            }
            LOG(INFO) << "Failed, the remaining retry times " << retry;
        }
    }
    {
        LOG(INFO) << "Check data...";
        std::vector<VertexID> vIds;
        std::vector<storage::cpp2::PropDef> retCols;
        for (int32_t vId = 0; vId < 10000; vId++) {
            vIds.emplace_back(vId);
        }
        retCols.emplace_back(storage::TestUtils::vertexPropDef("c", tagId));
        auto f = sClient->getVertexProps(spaceId, std::move(vIds), std::move(retCols));
        auto resp = std::move(f).get();
        if (!resp.succeeded()) {
            std::stringstream ss;
            for (auto& p : resp.failedParts()) {
                ss << "Part " << p.first
                   << ": " << apache::thrift::util::enumNameSafe(p.second)
                   << "; ";
            }
            VLOG(2) << "Failed partitions:: " << ss.str();
        }
        ASSERT_TRUE(resp.succeeded());
        auto& results = resp.responses();
        EXPECT_EQ(partition, results.size());
        EXPECT_EQ(0, results[0].result.failed_codes.size());
        EXPECT_EQ(1, results[0].vertex_schema[tagId].columns.size());
        auto tagProvider = std::make_shared<ResultSchemaProvider>(results[0].vertex_schema[tagId]);
        EXPECT_EQ(10000, results[0].vertices.size());
    }
    LOG(INFO) << "Let's open a new storage service";
    std::unique_ptr<test::ServerContext> newServer;
    std::unique_ptr<meta::MetaClient> newMetaClient;
    uint32_t storagePort = network::NetworkUtils::getAvailablePort();
    HostAddr storageAddr(localIp, storagePort);
    {
        MetaClientOptions options;
        options.localHost_ = storageAddr;
        options.clusterId_ = kClusterId;
        options.role_ = meta::cpp2::HostRole::STORAGE;
        newMetaClient = std::make_unique<meta::MetaClient>(threadPool,
                                                           metaAddr,
                                                           options);
        newMetaClient->waitForMetadReady();
        std::string dataPath = folly::stringPrintf("%s/%d/data", rootPath.path(), replica + 1);
        newServer = storage::TestUtils::mockStorageServer(newMetaClient.get(),
                                                          dataPath.c_str(),
                                                          localIp,
                                                          storagePort,
                                                          true);
        LOG(INFO) << "Start a new storage server on " << storageAddr;
    }
    LOG(INFO) << "Let's stop the last storage servcie " << storagePorts.back();
    {
        metaClients.back()->stop();
        serverContexts.back().reset();
        // Wait for the host be expired on meta.
        sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    }

    LOG(INFO) << "Let's balance";
    auto bIdRet = balancer.balance();
    CHECK(ok(bIdRet));
    while (balancer.isRunning()) {
        sleep(1);
    }

    // Sleep enough time until the data committed on newly added server
    sleep(FLAGS_raft_heartbeat_interval_secs + 5);
    {
        LOG(INFO) << "Balance Finished, check the newly added server";
        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = NebulaKeyUtils::prefix(1);
        auto partRet = newServer->kvStore_->part(spaceId, 1);
        CHECK(ok(partRet));
        auto part = value(partRet);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, part->engine()->prefix(prefix, &iter));
        int num = 0;
        std::string lastKey = "";
        while (iter->valid()) {
            // filter the multipule versions for data.
            auto key = NebulaKeyUtils::keyWithNoVersion(iter->key());
            if (lastKey == key) {
                iter->next();
                continue;
            }
            lastKey = key.str();
            iter->next();
            num++;
        }
        ASSERT_EQ(10000, num);
    }
    {
        LOG(INFO) << "Check meta";
        auto paRet = mClient->getPartsAlloc(spaceId).get();
        ASSERT_TRUE(paRet.ok()) << paRet.status();
        ASSERT_EQ(1, paRet.value().size());
        for (auto it = paRet.value().begin(); it != paRet.value().end(); it++) {
            ASSERT_EQ(3, it->second.size());
            ASSERT_TRUE(std::find(it->second.begin(), it->second.end(), storageAddr)
                                    != it->second.end());
        }
    }
    for (auto& c : metaClients) {
        c->stop();
    }
    serverContexts.clear();
    metaClients.clear();
    newMetaClient->stop();
    newServer.reset();
    newMetaClient.reset();
}

TEST(BalanceIntegrationTest, LeaderBalanceTest) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_raft_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/balance_integration_test.XXXXXX");
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    const nebula::ClusterID kClusterId = 10;

    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();
    LOG(INFO) << "Start meta server....";
    std::string metaPath = folly::stringPrintf("%s/meta", rootPath.path());
    auto metaServerContext = meta::TestUtils::mockMetaServer(localMetaPort, metaPath.c_str(),
                                                             kClusterId);
    localMetaPort = metaServerContext->port_;

    auto adminClient = std::make_unique<AdminClient>(metaServerContext->kvStore_.get());
    Balancer balancer(metaServerContext->kvStore_.get(), std::move(adminClient));

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(10);
    std::vector<HostAddr> metaAddr = {HostAddr(localIp, localMetaPort)};

    LOG(INFO) << "Create meta client...";
    auto mClient = std::make_unique<meta::MetaClient>(threadPool,
                                                      metaAddr);

    mClient->waitForMetadReady();

    int partition = 9;
    int replica = 3;
    std::vector<HostAddr> peers;
    std::vector<uint32_t> storagePorts;
    std::vector<std::shared_ptr<meta::MetaClient>> metaClients;

    std::vector<std::unique_ptr<test::ServerContext>> serverContexts;
    for (int i = 0; i < replica; i++) {
        uint32_t storagePort = network::NetworkUtils::getAvailablePort();
        HostAddr storageAddr(localIp, storagePort);
        storagePorts.emplace_back(storagePort);
        peers.emplace_back(storageAddr);

        VLOG(1) << "The storage server has been added to the meta service";
        MetaClientOptions options;
        options.localHost_ = storageAddr;
        options.clusterId_ = kClusterId;
        options.role_ = meta::cpp2::HostRole::STORAGE;
        auto metaClient = std::make_shared<meta::MetaClient>(threadPool,
                                                             metaAddr,
                                                             options);
        metaClient->waitForMetadReady();
        metaClients.emplace_back(metaClient);
    }

    for (int i = 0; i < replica; i++) {
        std::string dataPath = folly::stringPrintf("%s/%d/data", rootPath.path(), i);
        auto sc = storage::TestUtils::mockStorageServer(metaClients[i].get(),
                                                        dataPath.c_str(),
                                                        localIp,
                                                        storagePorts[i],
                                                        true);
        serverContexts.emplace_back(std::move(sc));
    }

    SpaceDesc spaceDesc("storage", partition, replica);
    auto ret = mClient->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok());
    while (true) {
        int totalLeaders = 0;
        for (int i = 0; i < replica; i++) {
            std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
            totalLeaders += serverContexts[i]->kvStore_->allLeader(leaderIds);
        }
        if (totalLeaders == partition) {
            break;
        }
        LOG(INFO) << "Waiting for leader election, current total leader number " << totalLeaders
                  << ", expected " << partition;
        sleep(1);
    }

    auto code = balancer.leaderBalance();
    ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

    LOG(INFO) << "Waiting for the leader balance";
    sleep(FLAGS_raft_heartbeat_interval_secs + 1);
    size_t leaderCount = 0;
    for (int i = 0; i < replica; i++) {
        std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
        leaderCount += serverContexts[i]->kvStore_->allLeader(leaderIds);
    }
    EXPECT_EQ(9, leaderCount);
    for (auto& c : metaClients) {
        c->stop();
    }
    serverContexts.clear();
    metaClients.clear();
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

