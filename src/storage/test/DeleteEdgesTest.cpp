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
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "utils/NebulaKeyUtils.h"


namespace nebula {
namespace storage {

TEST(DeleteEdgesTest, SimpleTest) {
    FLAGS_enable_multi_versions = true;
    fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto schemaMan = TestUtils::mockSchemaMan();
    auto indexMan = TestUtils::mockIndexMan();
    // Add edges
    {
        auto* processor = AddEdgesProcessor::instance(kv.get(),
                                                      schemaMan.get(),
                                                      indexMan.get(),
                                                      nullptr);
        cpp2::AddEdgesRequest req;
        req.space_id = 0;
        req.overwritable = true;
        for (PartitionID partId = 1; partId <= 3; partId++) {
            auto edges = TestUtils::setupEdges(partId, partId * 10, 10 * (partId + 1));
            req.parts.emplace(partId, std::move(edges));
        }

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
    // Add multi version edges
    {
        auto* processor = AddEdgesProcessor::instance(kv.get(),
                                                      schemaMan.get(),
                                                      indexMan.get(),
                                                      nullptr);
        cpp2::AddEdgesRequest req;
        req.space_id = 0;
        req.overwritable = true;
        // partId => List<Edge>
        // Edge => {EdgeKey, props}
        for (PartitionID partId = 1; partId <= 3; partId++) {
            auto edges = TestUtils::setupEdges(partId,
                                               partId * 10,
                                               10 * (partId + 1),
                                               101,
                                               1,
                                               "%d_%d_%ld_%ld_%d_%ld_new");
            req.parts.emplace(partId, std::move(edges));
        }

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }

    for (PartitionID partId = 1; partId <= 3; partId++) {
        for (VertexID srcId = 10 * partId; srcId < 10 * (partId + 1); srcId++) {
            auto prefix = NebulaKeyUtils::edgePrefix(partId, srcId, 101);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int num = 0;
            while (iter->valid()) {
                auto edgeType = 101;
                auto dstId = srcId * 100 + 2;
                if (num == 0) {
                    EXPECT_EQ(TestUtils::encodeValue(partId, srcId, dstId, edgeType, 0,
                                                     "%d_%d_%ld_%ld_%d_%ld_new"),
                              iter->val().str());
                } else {
                    EXPECT_EQ(TestUtils::encodeValue(partId, srcId, dstId, edgeType),
                              iter->val().str());
                }
                num++;
                iter->next();
            }
            EXPECT_EQ(2, num);
        }
    }

    // Delete edges
    {
        auto* processor = DeleteEdgesProcessor::instance(kv.get(), schemaMan.get(), indexMan.get());
        cpp2::DeleteEdgesRequest req;
        req.set_space_id(0);
        // partId => List<EdgeKey>
        for (PartitionID partId = 1; partId <= 3; partId++) {
            std::vector<cpp2::EdgeKey> keys;
            for (VertexID srcId = partId * 10; srcId < 10 * (partId + 1); srcId++) {
                EdgeType edgeType = srcId * 100 + 1;
                VertexID dstId = srcId * 100 + 2;
                EdgeRanking ranking = srcId * 100 + 3;
                cpp2::EdgeKey key;
                key.set_src(srcId);
                key.set_edge_type(edgeType);
                key.set_ranking(ranking);
                key.set_dst(dstId);
                keys.emplace_back(std::move(key));
            }
            req.parts.emplace(partId, std::move(keys));
        }

        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }

    for (PartitionID partId = 1; partId <= 3; partId++) {
        for (VertexID srcId = 10 * partId; srcId < 10 * (partId + 1); srcId++) {
            auto prefix = NebulaKeyUtils::vertexPrefix(partId, srcId, srcId * 100 + 1);
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

