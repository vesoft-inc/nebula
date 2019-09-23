/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>
#include "meta/processors/admin/Balancer.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "fs/TempDir.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace meta {

TEST(BalanceIntegrationTest, SimpleTest) {
    auto sc = std::make_unique<test::ServerContext>();
    auto handler = std::make_shared<nebula::storage::StorageServiceHandler>(nullptr, nullptr);
    sc->mockCommon("storage", 0, handler);
    LOG(INFO) << "Start storage server on " << sc->port_;
}

TEST(BalanceIntegrationTest, LeaderBalanceTest) {
    FLAGS_load_data_interval_secs = 1;
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
    uint32_t tempDataPort = network::NetworkUtils::getAvailablePort();
    HostAddr tempDataAddr(localIp, tempDataPort);
    auto mClient = std::make_unique<meta::MetaClient>(threadPool, metaAddr, tempDataAddr,
                                                      kClusterId, false);

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

        auto metaClient = std::make_shared<meta::MetaClient>(threadPool, metaAddr, storageAddr,
                                                             kClusterId, true);
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

    auto ret = mClient->createSpace("storage", partition, replica).get();
    ASSERT_TRUE(ret.ok());
    sleep(FLAGS_load_data_interval_secs + FLAGS_raft_heartbeat_interval_secs + 3);

    auto code = balancer.leaderBalance();
    ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

    LOG(INFO) << "Waiting for the leader balance";
    sleep(FLAGS_raft_heartbeat_interval_secs + 1);
    for (int i = 0; i < replica; i++) {
        std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
        EXPECT_EQ(3, serverContexts[i]->kvStore_->allLeader(leaderIds));
    }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


