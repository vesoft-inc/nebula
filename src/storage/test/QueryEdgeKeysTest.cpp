/* Copyright (c) 2019 vesoft inc. All rights reserved.
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
#include "storage/query/QueryEdgeKeysProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"

namespace nebula {
namespace storage {

TEST(QueryEdgeKeysTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/QueryEdgeKeysTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
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
        // partId => List<Edge>
        // Edge => {EdgeKey, props}
        for (PartitionID partId = 1; partId <= 3; partId++) {
            auto edges = TestUtils::setupEdges(partId, partId * 10, 10);
            for (VertexID srcId = partId * 10; srcId < 10 * (partId + 1); srcId++) {
                cpp2::EdgeKey key;
                key.set_src(srcId);
                key.set_edge_type(srcId * 100 + 1);
                key.set_ranking(srcId * 100 + 2);
                key.set_dst(srcId * 100 + 3);
                edges.emplace_back();
                edges.back().set_key(std::move(key));
                edges.back().set_props(folly::stringPrintf("%d_%ld", partId, srcId));
            }
            req.parts.emplace(partId, std::move(edges));
        }
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }

    LOG(INFO) << "Check data in kv store...";
    for (PartitionID partId = 1; partId <= 3; partId++) {
        for (VertexID srcId = 10 * partId; srcId < 10 * (partId + 1); srcId++) {
            auto prefix = NebulaKeyUtils::edgePrefix(partId, srcId, srcId * 100 + 1);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int num = 0;
            while (iter->valid()) {
                EXPECT_EQ(folly::stringPrintf("%d_%ld", partId, srcId), iter->val());
                num++;
                iter->next();
            }
            EXPECT_EQ(1, num);
        }
    }

    // Query edgekeys
    {
        cpp2::EdgeKeysRequest req;
        req.set_space_id(0);
        for (PartitionID partId = 1; partId <= 3; partId++) {
            std::vector<VertexID> verticse;
            for (VertexID srcId = 10 * partId; srcId < 10 * (partId + 1); srcId++) {
                verticse.emplace_back(srcId);
            }
            req.parts.emplace(partId, std::move(verticse));
        }

        auto* processor = QueryEdgeKeysProcessor::instance(kv.get(), nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
        CHECK_EQ(30, resp.edge_keys.size());

        VertexID srcId = 10;
        auto edge = resp.edge_keys[srcId][0];
        CHECK_EQ(srcId, edge.get_src());
        CHECK_EQ(srcId * 100 + 1, edge.get_edge_type());
        CHECK_EQ(srcId * 100 + 2, edge.get_ranking());
        CHECK_EQ(srcId * 100 + 3, edge.get_dst());
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


