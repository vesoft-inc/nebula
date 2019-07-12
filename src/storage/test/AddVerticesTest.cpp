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
#include "storage/AddVerticesProcessor.h"

namespace nebula {
namespace storage {

TEST(AddVerticesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    auto schemaMan = new AdHocSchemaManager();
    auto* processor = AddVerticesProcessor::instance(kv.get(), schemaMan);

    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req;
    req.space_id = 0;
    req.overwritable = true;
    // partId => List<Vertex>
    // Vertex => {Id, List<VertexProp>}
    // VertexProp => {tagId, tags}
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

    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_codes.size());

    LOG(INFO) << "Check data in kv store...";
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
            auto prefix = NebulaKeyUtils::vertexPrefix(partId, vertexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            TagID tagId = 0;
            while (iter->valid()) {
                EXPECT_EQ(folly::stringPrintf("%d_%d_%d", partId, vertexId, tagId), iter->val());
                tagId++;
                iter->next();
            }
            EXPECT_EQ(10, tagId);
        }
    }
}


TEST(AddVerticesTest, VersionTest) {
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    auto addVertices = [&](nebula::meta::SchemaManager* schemaMan, PartitionID partId,
                           VertexID srcId) {
        for (auto version = 1; version <= 10000; version++) {
            auto* processor = AddVerticesProcessor::instance(kv.get(), schemaMan);
            cpp2::AddVerticesRequest req;
            req.space_id = 0;
            req.overwritable = false;

            std::vector<cpp2::Vertex> vertices;
            std::vector<cpp2::Tag> tags;
            tags.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                              srcId*100 + 1,
                              folly::stringPrintf("%d_%ld_%d", partId, srcId, version));
            vertices.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                  srcId,
                                  std::move(tags));
            req.parts.emplace(partId, std::move(vertices));

            auto fut = processor->getFuture();
            processor->process(req);
            auto resp = std::move(fut).get();
            EXPECT_EQ(0, resp.result.failed_codes.size());
        }
    };

    auto checkVertexByPrefix = [&](PartitionID partId, VertexID vertexId,
                                   int32_t startValue, int32_t expectedNum) {
        auto prefix = NebulaKeyUtils::prefix(partId, vertexId);
        std::unique_ptr<kvstore::KVIterator> iter;
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
        int num = 0;
        while (iter->valid()) {
            EXPECT_EQ(folly::stringPrintf("%d_%ld_%d", partId, vertexId, startValue - num),
                      iter->val());
            num++;
            iter->next();
        }
        EXPECT_EQ(expectedNum, num);
    };

    auto checkVertexByRange = [&](PartitionID partId, VertexID vertexId,
                                  TagID tagId, int32_t startValue, int32_t expectedNum) {
        auto start = NebulaKeyUtils::vertexKey(partId,
                                               vertexId,
                                               tagId,
                                               0);
        auto end = NebulaKeyUtils::vertexKey(partId,
                                               vertexId,
                                               tagId,
                                               std::numeric_limits<int64_t>::max());
        std::unique_ptr<kvstore::KVIterator> iter;
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, kv->range(0, partId, start, end, &iter));
        int num = 0;
        while (iter->valid()) {
            EXPECT_EQ(folly::stringPrintf("%d_%ld_%d", partId, vertexId, startValue - num),
                      iter->val());
            num++;
            iter->next();
        }
        EXPECT_EQ(expectedNum, num);
    };

    {
        PartitionID partitionId = 0;
        VertexID srcId = 100;
        auto* schemaMan = new AdHocSchemaManager();
        schemaMan->setSpaceTimeSeries(0, true);

        LOG(INFO) << "Add vertices with multiple versions...";
        addVertices(schemaMan, partitionId, srcId);
        LOG(INFO) << "Check data in kv store...";
        checkVertexByPrefix(partitionId, srcId, 10000, 10000);
        checkVertexByRange(partitionId, srcId, srcId*100 + 1, 10000, 10000);
    }

    {
        PartitionID partitionId = 0;
        VertexID srcId = 101;
        auto* schemaMan = new AdHocSchemaManager();
        schemaMan->setSpaceTimeSeries(0, false);

        LOG(INFO) << "Add vertices with only one version...";
        addVertices(schemaMan, partitionId, srcId);
        LOG(INFO) << "Check data in kv store...";
        checkVertexByPrefix(partitionId, srcId, 10000, 1);
        checkVertexByRange(partitionId, srcId, srcId*100 + 1, 10000, 1);
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


