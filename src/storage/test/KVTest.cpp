/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/client/StorageClient.h"
#include "network/NetworkUtils.h"

DECLARE_string(meta_server_addrs);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {

void checkResult(StorageRpcResponse<storage::cpp2::GeneralResponse>& resp, size_t expectCount) {
    size_t count = 0;
    for (const auto& result : resp.responses()) {
        count += result.values.size();
        for (const auto& pair : result.values) {
            EXPECT_EQ(pair.first, pair.second);
        }
    }
    EXPECT_EQ(expectCount, count);
}

TEST(KVTest, GetPutInterfacesTest) {
    FLAGS_heartbeat_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    fs::TempDir rootPath("/tmp/KVTest.XXXXXX");
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
    meta::MetaClientOptions options;
    options.localHost_ = localHost;
    options.clusterId_ = kClusterId;
    options.inStoraged_ = true;
    auto mClient = std::make_unique<meta::MetaClient>(threadPool,
                                                      std::move(addrs),
                                                      options);
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
                                           false);

    meta::SpaceDesc spaceDesc("default", 10, 1);
    auto ret = mClient->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    spaceId = ret.value();
    LOG(INFO) << "Created space \"default\", its id is " << spaceId;
    sleep(FLAGS_heartbeat_interval_secs + 1);
    TestUtils::waitUntilAllElected(sc->kvStore_.get(), spaceId, 10);

    auto client = std::make_unique<StorageClient>(threadPool, mClient.get());
    // kv interface test
    {
        std::vector<nebula::cpp2::Pair> pairs;
        for (int32_t i = 0; i < 10; i++) {
            auto key = std::to_string(i);
            auto value = std::to_string(i);
            nebula::cpp2::Pair pair;
            pair.set_key(std::move(key));
            pair.set_value(std::move(value));
            pairs.emplace_back(std::move(pair));
        }
        auto future = client->put(spaceId, std::move(pairs));
        auto resp = std::move(future).get();
        ASSERT_TRUE(resp.succeeded());
        LOG(INFO) << "Put Successfully";
    }
    {
        std::vector<std::string> keys;
        for (int32_t i = 0; i < 10; i++) {
            keys.emplace_back(std::to_string(i));
        }

        auto future = client->get(spaceId, std::move(keys), false);
        auto resp = std::move(future).get();
        ASSERT_TRUE(resp.succeeded());
        LOG(INFO) << "Get Successfully";
        checkResult(resp, 10);
    }
    {
        std::vector<std::string> keys;
        for (int32_t i = 0; i < 20; i++) {
            keys.emplace_back(std::to_string(i));
        }

        auto future = client->get(spaceId, std::move(keys), false);
        auto resp = std::move(future).get();
        ASSERT_FALSE(resp.succeeded());
        LOG(INFO) << "Get failed because some key not exists";
        if (!resp.failedParts().empty()) {
            for (const auto& partEntry : resp.failedParts()) {
                EXPECT_EQ(partEntry.second, cpp2::ErrorCode::E_PARTIAL_RESULT);
            }
        }
        // Can not checkResult, because some part get all keys indeed, and other
        // part return E_PARTIAL_RESULT
    }
    {
        std::vector<std::string> keys;
        for (int32_t i = 0; i < 20; i++) {
            keys.emplace_back(std::to_string(i));
        }

        auto future = client->get(spaceId, std::move(keys), true);
        auto resp = std::move(future).get();
        ASSERT_TRUE(resp.succeeded());
        LOG(INFO) << "Get Successfully";
        checkResult(resp, 10);
    }
    {
        // try to get keys all not exists
        std::vector<std::string> keys;
        for (int32_t i = 10; i < 20; i++) {
            keys.emplace_back(std::to_string(i));
        }

        auto future = client->get(spaceId, std::move(keys), true);
        auto resp = std::move(future).get();
        ASSERT_TRUE(resp.succeeded());
        LOG(INFO) << "Get failed because some key not exists";
        if (!resp.failedParts().empty()) {
            for (const auto& partEntry : resp.failedParts()) {
                EXPECT_EQ(partEntry.second, cpp2::ErrorCode::E_PARTIAL_RESULT);
            }
        }
        checkResult(resp, 0);
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

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
