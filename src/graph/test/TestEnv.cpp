/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "meta/ClusterManager.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_string(meta_server_addrs);

namespace nebula {
namespace graph {


TestEnv *gEnv = nullptr;

TestEnv::TestEnv() {
}


TestEnv::~TestEnv() {
}


void TestEnv::SetUp() {
    FLAGS_load_data_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    // Create metaServer
    metaServer_ = nebula::meta::TestUtils::mockMetaServer(
                                                    network::NetworkUtils::getAvailablePort(),
                                                    metaRootPath_.path(),
                                                    kClusterId);
    FLAGS_meta_server_addrs = folly::stringPrintf("127.0.0.1:%d", metaServerPort());

    // Create storageServer
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto addrsRet
        = network::NetworkUtils::toHosts(folly::stringPrintf("127.0.0.1:%d", metaServerPort()));
    CHECK(addrsRet.ok()) << addrsRet.status();
    auto storagePort = network::NetworkUtils::getAvailablePort();
    auto hostRet = nebula::network::NetworkUtils::toHostAddr("127.0.0.1", storagePort);
    if (!hostRet.ok()) {
        LOG(ERROR) << "Bad local host addr, status:" << hostRet.status();
    }
    auto& localhost = hostRet.value();

    auto clusterMan
        = std::make_unique<nebula::meta::ClusterManager>("", "", kClusterId);
    mClient_ = std::make_unique<meta::MetaClient>(threadPool,
                                                  std::move(addrsRet.value()),
                                                  localhost,
                                                  clusterMan.get(),
                                                  true);
    auto r = mClient_->addHosts({localhost}).get();
    ASSERT_TRUE(r.ok());
    mClient_->waitForMetadReady();
    r = mClient_->removeHosts({localhost}).get();
    ASSERT_TRUE(r.ok());
    gflagsManager_ = std::make_unique<meta::ClientBasedGflagsManager>(mClient_.get());
    IPv4 localIp;
    nebula::network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    storageServer_ = nebula::storage::TestUtils::mockStorageServer(
                                                        mClient_.get(),
                                                        storageRootPath_.path(),
                                                        localIp,
                                                        storagePort,
                                                        true);

    // Create graphServer
    graphServer_ = TestUtils::mockGraphServer(0);
}


void TestEnv::TearDown() {
    // TO make sure the drop space be invoked on storage server
    sleep(FLAGS_load_data_interval_secs + 1);
    graphServer_.reset();
    storageServer_.reset();
    mClient_.reset();
    metaServer_.reset();
    mClient_.reset();
}

uint16_t TestEnv::graphServerPort() const {
    return graphServer_->port_;
}

uint16_t TestEnv::metaServerPort() const {
    return metaServer_->port_;
}

uint16_t TestEnv::storageServerPort() const {
    return storageServer_->port_;
}

std::unique_ptr<GraphClient> TestEnv::getClient() const {
    auto client = std::make_unique<GraphClient>("127.0.0.1", graphServerPort());
    if (cpp2::ErrorCode::SUCCEEDED != client->connect("user", "password")) {
        return nullptr;
    }
    return client;
}

meta::ClientBasedGflagsManager* TestEnv::gflagsManager() {
    return gflagsManager_.get();
}

}   // namespace graph
}   // namespace nebula
