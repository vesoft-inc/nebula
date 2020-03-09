/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/mutate/AddVerticesProcessor.h"

namespace nebula {
namespace storage {

TEST(AddVerticesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    auto schemaMan = TestUtils::mockSchemaMan();
    auto indexMan = TestUtils::mockIndexMan();
    auto* processor = AddVerticesProcessor::instance(kv.get(),
                                                     schemaMan.get(),
                                                     indexMan.get(),
                                                     nullptr);

    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req;
    req.space_id = 0;
    req.overwritable = true;
    // partId => List<Vertex>
    // Vertex => {Id, List<VertexProp>}
    // VertexProp => {tagId, tags}
    for (PartitionID partId = 0; partId < 3; partId++) {
        auto vertices = TestUtils::setupVertices(partId, partId * 10, 10 * (partId + 1));
        req.parts.emplace(partId, std::move(vertices));
    }

    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_codes.size());

    LOG(INFO) << "Check data in kv store...";
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

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


