/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

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
    // Create metaServer
    metaServer_ = nebula::meta::TestUtils::mockMetaServer(
                                                    network::NetworkUtils::getAvailablePort(),
                                                    metaRootPath_.path());
    FLAGS_meta_server_addrs = folly::stringPrintf("127.0.0.1:%d", metaServerPort());

    // Create storageServer
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto addrsRet
        = network::NetworkUtils::toHosts(folly::stringPrintf("127.0.0.1:%d", metaServerPort()));
    CHECK(addrsRet.ok()) << addrsRet.status();
    mClient_ = std::make_unique<meta::MetaClient>(threadPool, std::move(addrsRet.value()), true);
    mClient_->init();
    IPv4 localIp;
    nebula::network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    storageServer_ = nebula::storage::TestUtils::mockStorageServer(
                                                        mClient_.get(),
                                                        storageRootPath_.path(),
                                                        localIp,
                                                        network::NetworkUtils::getAvailablePort(),
                                                        true);

    // Create graphServer
    graphServer_ = TestUtils::mockGraphServer(0);
}


void TestEnv::TearDown() {
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

std::unique_ptr<GraphClient> TestEnv::getClient() const {
    auto client = std::make_unique<GraphClient>("127.0.0.1", graphServerPort());
    if (cpp2::ErrorCode::SUCCEEDED != client->connect("user", "password")) {
        return nullptr;
    }
    return client;
}

}   // namespace graph
}   // namespace nebula
