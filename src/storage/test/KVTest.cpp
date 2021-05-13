/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/network/NetworkUtils.h"
#include <gtest/gtest.h>
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "common/clients/storage/GeneralStorageClient.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/kv/PutProcessor.h"
#include "storage/kv/GetProcessor.h"
#include "storage/kv/RemoveProcessor.h"

DECLARE_string(meta_server_addrs);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {

TEST(KVTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/KVSimpleTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    const GraphSpaceID space = 1;
    const int32_t totalParts = 6;
    {
        auto* processor = PutProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        auto req = mock::MockData::mockKVPut();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }
    {
        auto* processor = GetProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        auto req = mock::MockData::mockKVGet();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        for (size_t part = 1; part <= totalParts; part++) {
            auto key =  NebulaKeyUtils::kvKey(part,
                                              folly::stringPrintf("key_%ld", part));
            std::string value;
            auto code = cluster.storageKV_->get(space, part, key, &value);
            EXPECT_EQ(folly::stringPrintf("value_%ld", part), value);
            EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        }
    }
    {
        auto* processor = RemoveProcessor::instance(env, nullptr);
        auto fut = processor->getFuture();
        auto req = mock::MockData::mockKVRemove();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        for (size_t part = 1; part <= totalParts; part++) {
            auto key =  NebulaKeyUtils::kvKey(part,
                                              folly::stringPrintf("key_%ld", part));
            std::string value;
            auto code = cluster.storageKV_->get(space, part, key, &value);
            EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, code);
        }
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
