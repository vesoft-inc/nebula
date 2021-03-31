/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/interface/gen-cpp2/common_types.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {

TEST(DeleteVerticesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
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

        LOG(INFO) << "Check data in kv store...";
        // The number of data in players and teams is 81
        checkAddVerticesData(req, env, 81, 0);
    }

    // Delete vertices
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();

        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check data in kv store...";
        auto ret = env->schemaMan_->getSpaceVidLen(req.get_space_id());
        EXPECT_TRUE(ret.ok());
        auto spaceVidLen = ret.value();

        // All the added datas are deleted, the number of vertices is 0
        checkVerticesData(spaceVidLen, req.get_space_id(), *req.parts_ref(), env, 0);
    }
}

TEST(DeleteVerticesTest, MultiVersionTest) {
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();

    // Add vertices
    {
        LOG(INFO) << "Build AddVerticesRequest...";
        cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
        cpp2::AddVerticesRequest specifiedOrderReq =
            mock::MockData::mockAddVerticesSpecifiedOrderReq();

        {
            LOG(INFO) << "AddVerticesProcessor...";
            auto* processor = AddVerticesProcessor::instance(env, nullptr);
            auto fut = processor->getFuture();
            processor->process(req);
            auto resp = std::move(fut).get();
            EXPECT_EQ(0, resp.result.failed_parts.size());
        }
        {
            LOG(INFO) << "AddVerticesProcessor...";
            auto* processor = AddVerticesProcessor::instance(env, nullptr);
            auto fut = processor->getFuture();
            processor->process(specifiedOrderReq);
            auto resp = std::move(fut).get();
            EXPECT_EQ(0, resp.result.failed_parts.size());
        }

        LOG(INFO) << "Check data in kv store...";
        // The number of vertices is 81
        checkAddVerticesData(req, env, 81, 2);
    }

    // Delete vertices
    {
        auto* processor = DeleteVerticesProcessor::instance(env, nullptr);

        LOG(INFO) << "Build DeleteVerticesRequest...";
        cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();

        LOG(INFO) << "Test DeleteVerticesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());

        LOG(INFO) << "Check data in kv store...";
        auto ret = env->schemaMan_->getSpaceVidLen(req.get_space_id());
        EXPECT_TRUE(ret.ok());
        auto spaceVidLen = ret.value();

        // All the added datas are deleted, the number of vertices is 0
        checkVerticesData(spaceVidLen, req.get_space_id(), *req.parts_ref(), env, 0);
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

