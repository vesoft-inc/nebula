/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "meta/RootUserMan.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(meta_server_addrs);

namespace nebula {
namespace graph {


TestEnv *gEnv = nullptr;

TestEnv::TestEnv() {
}


TestEnv::~TestEnv() {
}


void TestEnv::SetUp() {
    FLAGS_heartbeat_interval_secs = 1;
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

    if (!nebula::meta::RootUserMan::initRootUser(metaServer_->kvStore_.get())) {
        LOG(ERROR) << "Init root user failed";
    }

    meta::MetaClientOptions options;
    options.localHost_ = localhost;
    options.clusterId_ = kClusterId;
    options.inStoraged_ = true;
    mClient_ = std::make_unique<meta::MetaClient>(threadPool,
                                                  std::move(addrsRet.value()),
                                                  options);
    auto ready = mClient_->waitForMetadReady(3);
    if (!ready) {
        // Resort to retrying in the background
        LOG(WARNING) << "Failed to synchronously wait for meta service ready";
    }
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
    sleep(FLAGS_heartbeat_interval_secs + 1);
    graphServer_.reset();
    storageServer_.reset();
    mClient_.reset();
    metaServer_.reset();
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

std::unique_ptr<GraphClient> TestEnv::getClient(const std::string& user,
                                                const std::string& password) const {
    auto client = std::make_unique<GraphClient>("127.0.0.1", graphServerPort());
    if (cpp2::ErrorCode::SUCCEEDED != client->connect(user, password)) {
        return nullptr;
    }
    return client;
}

meta::ClientBasedGflagsManager* TestEnv::gflagsManager() {
    return gflagsManager_.get();
}

test::ServerContext* TestEnv::storageServer() {
    return storageServer_.get();
}

meta::MetaClient* TestEnv::metaClient() {
    return mClient_.get();
}

}   // namespace graph
}   // namespace nebula
