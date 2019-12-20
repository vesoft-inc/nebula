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
#include "storage/mutate/DeleteVertexProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "base/NebulaKeyUtils.h"


namespace nebula {
namespace storage {

TEST(DeleteVertexTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
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

        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
                auto prefix = NebulaKeyUtils::vertexPrefix(partId, vertexId);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
                TagID tagId = 0;
                while (iter->valid()) {
                    EXPECT_EQ(folly::stringPrintf("%d_%d_%d",
                                                   partId, vertexId, tagId), iter->val());
                    tagId++;
                    iter->next();
                }
                EXPECT_EQ(10, tagId);
            }
        }
    }

    // Delete vertices
    {
        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
                auto* processor = DeleteVertexProcessor::instance(kv.get(), nullptr, nullptr);
                cpp2::DeleteVertexRequest req;
                req.set_space_id(0);
                req.set_part_id(partId);
                req.set_vid(vertexId);

                auto fut = processor->getFuture();
                processor->process(req);
                auto resp = std::move(fut).get();
                EXPECT_EQ(0, resp.result.failed_codes.size());
            }
        }
    }

    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
            auto prefix = NebulaKeyUtils::vertexPrefix(partId, vertexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            CHECK(!iter->valid());
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

