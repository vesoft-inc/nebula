/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/datatypes/KeyValue.h"
#include "common/network/NetworkUtils.h"
#include "common/clients/storage/GeneralStorageClient.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

DECLARE_string(meta_server_addrs);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {

void checkResult(StorageRpcResponse<storage::cpp2::KVGetResponse>& resp, size_t expectCount) {
    size_t count = 0;
    for (const auto& result : resp.responses()) {
        count += (*result.key_values_ref()).size();
        for (const auto& pair : *result.key_values_ref()) {
            EXPECT_EQ(pair.first, pair.second);
        }
    }
    EXPECT_EQ(expectCount, count);
}

TEST(KVClientTest, SimpleTest) {
    GraphSpaceID spaceId = 1;
    fs::TempDir metaPath("/tmp/KVTest.meta.XXXXXX");
    fs::TempDir stoagePath("/tmp/KVTest.stoage.XXXXXX");
    mock::MockCluster cluster;
    std::string storageName{"127.0.0.1"};
    auto storagePort = network::NetworkUtils::getAvailablePort();
    HostAddr storageAddr{storageName, storagePort};

    cluster.startMeta(metaPath.path());
    meta::MetaClientOptions options;
    options.localHost_ = storageAddr;
    options.role_ = meta::cpp2::HostRole::STORAGE;
    cluster.initMetaClient(options);
    cluster.startStorage(storageAddr, stoagePath.path(), true);

    auto client = cluster.initGeneralStorageClient();
    // kv interface test
    {
        std::vector<nebula::KeyValue> pairs;
        for (int32_t i = 0; i < 10; i++) {
            auto key = std::to_string(i);
            auto value = std::to_string(i);
            nebula::KeyValue pair(std::make_pair(key, value));
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
                EXPECT_EQ(partEntry.second, nebula::cpp2::ErrorCode::E_PARTIAL_RESULT);
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
                EXPECT_EQ(partEntry.second, nebula::cpp2::ErrorCode::E_PARTIAL_RESULT);
            }
        }
        checkResult(resp, 0);
    }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
