/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/AddVerticesProcessor.h"
#include "storage/KeyUtils.h"

namespace nebula {
namespace storage {

TEST(AddVerticesTest, SimpleTest) {
    auto workers = std::make_shared<thread::GenericThreadPool>();
    workers->start(4);
    auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(),
                                                             {0, 0},
                                                             ioPool,
                                                             workers);
    auto* processor = AddVerticesProcessor::instance(kv.get(), nullptr);

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
            auto prefix = KeyUtils::prefix(partId, vertexId);
            auto res = kv->prefix(0, partId, prefix);
            ASSERT_TRUE(ok(res));

            std::unique_ptr<kvstore::KVIterator> iter = value(std::move(res));
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

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


