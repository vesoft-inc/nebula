/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "utils/NebulaKeyUtils.h"
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
    auto schemaMan = TestUtils::mockSchemaMan();
    auto indexMan = TestUtils::mockIndexMan();
    auto* processor = AddEdgesProcessor::instance(kv.get(),
                                                  schemaMan.get(),
                                                  indexMan.get(),
                                                  nullptr);

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req;
    req.space_id = 0;
    req.overwritable = true;
    // partId => List<Edge>
    // Edge => {EdgeKey, props}
    for (PartitionID partId = 1; partId <= 3; partId++) {
        auto edges = TestUtils::setupEdges(partId, partId * 10, 10 * (partId + 1));
        req.parts.emplace(partId, std::move(edges));
    }

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_codes.size());

    LOG(INFO) << "Check data in kv store...";
    for (PartitionID partId = 1; partId <= 3; partId++) {
        for (VertexID srcId = 10 * partId; srcId < 10 * (partId + 1); srcId++) {
            auto prefix = NebulaKeyUtils::edgePrefix(partId, srcId, 101);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int num = 0;
            while (iter->valid()) {
                auto edgeType = 101;
                auto dstId = srcId * 100 + 2;
                EXPECT_EQ(TestUtils::encodeValue(partId, srcId, dstId, edgeType),
                          iter->val().str());
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


