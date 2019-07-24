/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "meta/client/MetaClient.h"
#include "meta/test/TestUtils.h"
#include "network/NetworkUtils.h"
#include "meta/MetaServiceUtils.h"
#include "meta/ServerBasedSchemaManager.h"
#include "dataman/ResultSchemaProvider.h"
#include "meta/test/TestUtils.h"
#include "meta/ClusterManager.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_int32(heartbeat_interval_secs);


namespace nebula {
namespace meta {

using nebula::ClusterID;
ClusterID gClusterId;
std::unique_ptr<std::string> gMetaClusterIdPath
    = std::make_unique<std::string>("/tmp/meta.cluster.id.");
std::unique_ptr<std::string> gClientClusterIdPath
    = std::make_unique<std::string>("/tmp/storage.cluster.id.");

TEST(ClusterManagerTest, ClusterIdTestIdEQ0) {
    FLAGS_load_data_interval_secs = 5;
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/ClusterManagerTest.XXXXXX");
    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();

    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());
    *gMetaClusterIdPath = sc->clusterIdPath_;
    bool ret = sc->clusterMan_->loadOrCreateCluId();
    ASSERT_TRUE(ret);

    gClusterId = sc->clusterMan_->getClusterId();

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};

    *gClientClusterIdPath += sc->curTime_;
    auto clientClusterMan
        = std::make_unique<nebula::meta::ClusterManager>("", *gClientClusterIdPath);
    if (!clientClusterMan->loadClusterId()) {
        LOG(ERROR) << "storaged clusterId load error!";
    }
    ASSERT_EQ(0, clientClusterMan->getClusterId());

    auto hosts = std::vector<HostAddr>{HostAddr(localIp, localMetaPort)};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::move(hosts),
                                               localHost,
                                               clientClusterMan.get(),
                                               true);  // send heartbeat
    client->addHosts({localHost});
    sleep(FLAGS_heartbeat_interval_secs);
    ret = client->waitForMetadReady(3);
    ASSERT_TRUE(ret);
    ASSERT_EQ(gClusterId, clientClusterMan->getClusterId());
}


TEST(ClusterManagerTest, ClusterIdTestIdMatch) {
    FLAGS_load_data_interval_secs = 5;
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/ClusterManagerTest.XXXXXX");
    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();

    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());
    bool ret = sc->clusterMan_->loadOrCreateCluId();
    ASSERT_TRUE(ret);

    auto clusterId = sc->clusterMan_->getClusterId();

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};

    std::string clientClusterIdPath = "/tmp/storage.cluster.id.";
    clientClusterIdPath += sc->curTime_;
    auto clientClusterMan
        = std::make_unique<nebula::meta::ClusterManager>("", clientClusterIdPath);
    clientClusterMan->setClusterId(clusterId);
    ret = clientClusterMan->dumpClusterId();
    ASSERT_TRUE(ret);

    auto hosts = std::vector<HostAddr>{HostAddr(localIp, localMetaPort)};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::move(hosts),
                                               localHost,
                                               clientClusterMan.get(),
                                               true);  // send heartbeat
    client->addHosts({localHost});
    sleep(FLAGS_heartbeat_interval_secs);
    ret = client->waitForMetadReady(3);
    ASSERT_TRUE(ret);
}


TEST(ClusterManagerTest, ClusterIdMismatchTest) {
    FLAGS_load_data_interval_secs = 5;
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/ClusterManagerTest.XXXXXX");
    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();

    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());
    bool ret = sc->clusterMan_->loadOrCreateCluId();
    ASSERT_TRUE(ret);

    auto clusterId = sc->clusterMan_->getClusterId();

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};

    std::string clientClusterIdPath = "/tmp/storage.cluster.id.";
    clientClusterIdPath += sc->curTime_;
    auto clientClusterMan
        = std::make_unique<nebula::meta::ClusterManager>("", clientClusterIdPath);
    clientClusterMan->setClusterId(clusterId + 1);
    ret = clientClusterMan->dumpClusterId();
    ASSERT_TRUE(ret);

    auto hosts = std::vector<HostAddr>{HostAddr(localIp, localMetaPort)};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::move(hosts),
                                               localHost,
                                               clientClusterMan.get(),
                                               true);  // send heartbeat
    client->addHosts({localHost});
    sleep(FLAGS_heartbeat_interval_secs);
    ret = client->waitForMetadReady(3);
    ASSERT_TRUE(!ret);
}


TEST(ClusterManagerTest, ClusterIdLoadTest) {
    auto metaClusterMan
        = std::make_unique<nebula::meta::ClusterManager>("", *gMetaClusterIdPath);
    bool ret = metaClusterMan->loadOrCreateCluId();
    ASSERT_TRUE(ret);
    ASSERT_EQ(gClusterId, metaClusterMan->getClusterId());

    auto clientClusterMan
        = std::make_unique<nebula::meta::ClusterManager>("", *gClientClusterIdPath);
    ret = clientClusterMan->loadClusterId();
    ASSERT_TRUE(ret);
    ASSERT_EQ(gClusterId, clientClusterMan->getClusterId());
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


