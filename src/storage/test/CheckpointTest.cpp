/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "storage/admin/CreateCheckpointProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace storage {
TEST(CheckpointTest, simpleTest) {
    fs::TempDir dataPath("/tmp/Checkpoint_Test_src.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(dataPath.path());
    auto* env = cluster.storageEnv_.get();
    // Add vertices
    {
        auto* processor = AddVerticesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();

        LOG(INFO) << "Test AddVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
    }

    // Begin checkpoint
    {
        auto* processor = CreateCheckpointProcessor::instance(env);
        cpp2::CreateCPRequest req;
        req.set_space_id(1);
        req.set_name("checkpoint_test");
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        auto checkpoint1 = folly::stringPrintf("%s/disk1/nebula/1/checkpoints/checkpoint_test/data",
                                               dataPath.path());
        auto files = fs::FileUtils::listAllFilesInDir(checkpoint1.data());
        ASSERT_EQ(4, files.size());
        files.clear();
        auto checkpoint2 = folly::stringPrintf("%s/disk2/nebula/1/checkpoints/checkpoint_test/data",
                                               dataPath.path());
        fs::FileUtils::listAllFilesInDir(checkpoint2.data());
        files = fs::FileUtils::listAllFilesInDir(checkpoint2.data());
        ASSERT_EQ(4, files.size());
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
