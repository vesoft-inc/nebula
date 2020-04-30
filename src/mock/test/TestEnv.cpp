/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "mock/test/TestEnv.h"

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
    mServer_ = std::make_unique<MockServer>();
    metaPort_ = mServer_->startMeta();
    FLAGS_meta_server_addrs = folly::stringPrintf("127.0.0.1:%d", metaPort_);
    storagePort_ = mServer_->startStorage(metaPort_);
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    HostAddr hostAddr;
    auto hostStatus = network::NetworkUtils::resolveHost("127.0.0.1", metaPort_);
    auto storageHost = network::NetworkUtils::toHostAddr("127.0.0.1", storagePort_);
    meta::MetaClientOptions options;
    options.localHost_ = storageHost.value();
    options.clusterId_ = 100;
    options.inStoraged_ = true;
    mClient_ = std::make_unique<meta::MetaClient>(threadPool,
            std::move(hostStatus).value(), options);
    mClient_->waitForMetadReady();
    sClient_ = std::make_unique<storage::GraphStorageClient>(threadPool, mClient_.get());
    graphPort_ = mServer_->startGraph();
}


void TestEnv::TearDown() {
    mClient_.reset();
    mServer_.reset();
}

uint16_t TestEnv::graphServerPort() const {
    return graphPort_;
}

uint16_t TestEnv::metaServerPort() const {
    return metaPort_;
}

uint16_t TestEnv::storageServerPort() const {
    return storagePort_;
}

std::unique_ptr<GraphClient> TestEnv::getGraphClient() const {
    auto client = std::make_unique<GraphClient>("127.0.0.1", graphServerPort());
    if (cpp2::ErrorCode::SUCCEEDED != client->connect("user", "password")) {
        return nullptr;
    }
    return client;
}

meta::MetaClient* TestEnv::getMetaClient() {
    return mClient_.get();
}

storage::GraphStorageClient* TestEnv::getStorageClient() {
    return sClient_.get();
}
}   // namespace graph
}   // namespace nebula
