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
#include "storage/mutate/AddEdgesProcessor.h"

namespace nebula {
namespace storage {

TEST(AddEdgesTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    auto* processor = AddEdgesProcessor::instance(kv.get(), nullptr, nullptr);

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req;
    req.space_id = 0;
    req.overwritable = true;
    // partId => List<Edge>
    // Edge => {EdgeKey, props}
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<cpp2::Edge> edges;
        for (auto srcId = partId * 10; srcId < 10 * (partId + 1); srcId++) {
            cpp2::EdgeKey key;
            key.set_src(srcId);
            key.set_edge_type(srcId * 100 + 1);
            key.set_ranking(srcId * 100 + 2);
            key.set_dst(srcId * 100 + 3);
            edges.emplace_back();
            edges.back().set_key(std::move(key));
            edges.back().set_props(folly::stringPrintf("%d_%d", partId, srcId));
        }
        req.parts.emplace(partId, std::move(edges));
    }

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_codes.size());

    LOG(INFO) << "Check data in kv store...";
    for (auto partId = 0; partId < 3; partId++) {
        for (auto srcId = 10 * partId; srcId < 10 * (partId + 1); srcId++) {
            auto prefix = NebulaKeyUtils::edgePrefix(partId, srcId, srcId*100 + 1);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int num = 0;
            while (iter->valid()) {
                EXPECT_EQ(folly::stringPrintf("%d_%d", partId, srcId), iter->val());
                num++;
                iter->next();
            }
            EXPECT_EQ(1, num);
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


