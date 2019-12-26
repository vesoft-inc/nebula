/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/StorageServiceHandler.h"

namespace nebula {
namespace storage {

TEST(StorageServiceHandlerTest, FutureAddVerticesTest) {
    fs::TempDir rootPath("/tmp/FutureAddVerticesTest.XXXXXX");
    cpp2::AddVerticesRequest req;
    req.set_space_id(0);
    req.overwritable = true;

    LOG(INFO) << "Build FutureAddVerticesTest...";
    req.parts.emplace(0, TestUtils::setupVertices(0, 10, 10));
    req.parts.emplace(1, TestUtils::setupVertices(1, 20, 30));
    LOG(INFO) << "Test FutureAddVerticesTest...";
    std::unique_ptr<kvstore::KVStore> kvstore = TestUtils::initKV(rootPath.path());

    auto storageServiceHandler = std::make_unique<StorageServiceHandler>(kvstore.get(),
                                                                         nullptr,
                                                                         nullptr);
    auto resp = storageServiceHandler->future_addVertices(req).get();
    EXPECT_EQ(typeid(cpp2::ExecResponse).name() , typeid(resp).name());

    LOG(INFO) << "Check ErrorCode of AddVerticesProcessor...";
    ASSERT_EQ(0, resp.result.failed_codes.size());

    LOG(INFO) << "Verify the vertices data...";
    auto prefix = NebulaKeyUtils::vertexPrefix(1, 19);
    std::unique_ptr<kvstore::KVIterator> iter;
    ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, kvstore->prefix(0, 1, prefix, &iter));
    TagID tagId = 0;
    while (iter->valid()) {
        ASSERT_EQ(folly::stringPrintf("%d_%d_%d", 1, 19, tagId), iter->val());
        tagId++;
        iter->next();
    }
    ASSERT_EQ(30, tagId);
    LOG(INFO) << "Test FutureAddVerticesTest...";
}
}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
