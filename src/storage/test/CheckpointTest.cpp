/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/admin/CreateCheckpointProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"

namespace nebula {
namespace storage {
TEST(CheckpointTest, simpleTest) {
    fs::TempDir dataPath("/tmp/Checkpoint_Test_src.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(dataPath.path()));
    // Add vertices
    {
        auto* processor = AddVerticesProcessor::instance(kv.get(), nullptr, nullptr);
        cpp2::AddVerticesRequest req;
        req.space_id = 0;
        req.overwritable = false;
        // partId => List<Vertex>
        for (auto partId = 0; partId < 3; partId++) {
            std::vector<cpp2::Vertex> vertices;
            for (auto vertexId = partId * 10; vertexId < 10 * (partId + 1); vertexId++) {
                std::vector<cpp2::Tag> tags;
                for (auto tagId = 0; tagId < 10; tagId++) {
                    tags.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                      tagId,
                                      folly::stringPrintf("%d_%d_%d", partId, vertexId, tagId));
                }
                vertices.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                      vertexId,
                                      std::move(tags));
            }
            req.parts.emplace(partId, std::move(vertices));
        }

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }

    // Begin checkpoint
    {
        auto* processor = CreateCheckpointProcessor::instance(kv.get());
        cpp2::CreateCPRequest req;
        req.name = "checkpoint_test";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
        auto checkpoint1 = folly::stringPrintf("%s/disk1/nebula/0/checkpoints/checkpoint_test/data",
                                               dataPath.path());
        auto files = fs::FileUtils::listAllFilesInDir(checkpoint1.data());
        ASSERT_EQ(4, files.size());
        files.clear();
        auto checkpoint2 = folly::stringPrintf("%s/disk2/nebula/0/checkpoints/checkpoint_test/data",
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
