/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/query/GetUUIDProcessor.h"

DECLARE_int32(max_handlers_per_req);

namespace nebula {
namespace storage {

void getUUID(kvstore::KVStore* kv, UUIDCache* cache, size_t start, size_t end) {
    for (size_t i = start; i < end; i++) {
        auto* processor = GetUUIDProcessor::instance(kv, cache);
        cpp2::GetUUIDReq req;
        req.set_space_id(0);
        req.set_part_id(i % 10);
        req.set_name(std::to_string(i));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
}

void checkCache(UUIDCache* cache, uint64_t evicts, uint64_t hits) {
    EXPECT_EQ(evicts, cache->evicts());
    EXPECT_EQ(hits, cache->hits());
}


TEST(UUIDCacheTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/UUIDCacheTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(), 10);
    auto schemaMan = TestUtils::mockSchemaMan();
    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(1);

    UUIDCache cache(1000, 0);

    LOG(INFO) << "Get uuid";
    getUUID(kv.get(), &cache, 0, 1000);
    checkCache(&cache, 0, 0);

    LOG(INFO) << "Get again";
    getUUID(kv.get(), &cache, 0, 1000);
    checkCache(&cache, 0, 1000);

    LOG(INFO) << "Get a part of cached";
    getUUID(kv.get(), &cache, 500, 1500);
    checkCache(&cache, 500, 1500);

    LOG(INFO) << "Get non-cached uuid";
    getUUID(kv.get(), &cache, 2000, 3000);
    checkCache(&cache, 1500, 1500);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


