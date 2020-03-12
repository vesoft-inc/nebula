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
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "utils/NebulaKeyUtils.h"


namespace nebula {
namespace storage {

TEST(DeleteVerticesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto schemaMan = TestUtils::mockSchemaMan();
    auto indexMan = TestUtils::mockIndexMan();
    auto* charsetInfo = CharsetInfo::instance();

    // Add vertices
    {
        auto* processor = AddVerticesProcessor::instance(kv.get(),
                                                         schemaMan.get(),
                                                         charsetInfo,
                                                         indexMan.get(),
                                                         nullptr);
        cpp2::AddVerticesRequest req;
        req.space_id = 0;
        req.overwritable = false;
        // partId => List<Vertex>
        for (PartitionID partId = 0; partId < 3; partId++) {
            auto vertices = TestUtils::setupVertices(partId, partId * 10, 10 * (partId + 1));
            req.parts.emplace(partId, std::move(vertices));
        }

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());

        for (PartitionID partId = 0; partId < 3; partId++) {
            for (VertexID vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
                auto prefix = NebulaKeyUtils::vertexPrefix(partId, vertexId);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
                TagID tagId = 0;
                while (iter->valid()) {
                    EXPECT_EQ(TestUtils::encodeValue(partId, vertexId, tagId), iter->val());
                    tagId++;
                    iter->next();
                }
                EXPECT_EQ(10, tagId);
            }
        }
    }

    // Delete vertices
    {
        cpp2::DeleteVerticesRequest req;
        req.set_space_id(0);
        for (PartitionID partId = 0; partId < 3; partId++) {
            std::vector<VertexID> vertices;
            for (VertexID vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
                vertices.emplace_back(vertexId);
            }
            req.parts.emplace(partId, std::move(vertices));
        }

        auto* processor = DeleteVerticesProcessor::instance(kv.get(),
                                                            schemaMan.get(),
                                                            charsetInfo,
                                                            indexMan.get(),
                                                            nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }

    for (PartitionID partId = 0; partId < 3; partId++) {
        for (VertexID vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
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

