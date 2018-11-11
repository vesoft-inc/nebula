/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <folly/lang/Bits.h>
#include "fs/TempDir.h"
#include "storage/RocksdbEngine.h"

namespace vesoft {
namespace storage {

TEST(RocksdbEngineTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_test.XXXXXX");
    std::unique_ptr<RocksdbEngine> engine = std::make_unique<RocksdbEngine>(0, rootPath.path());
    EXPECT_EQ(ResultCode::SUCCESSED, engine->put("key", "val"));
    std::string val;
    EXPECT_EQ(ResultCode::SUCCESSED, engine->get("key", val));
    EXPECT_EQ(val, "val");
}

TEST(RocksdbEngineTest, RangeTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_test.XXXXXX");
    std::unique_ptr<RocksdbEngine> engine = std::make_unique<RocksdbEngine>(0, rootPath.path());
    std::vector<KV> data;
    for (int32_t i = 10; i < 20;  i++) {
        data.emplace_back(std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t)),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(ResultCode::SUCCESSED, engine->multiPut(std::move(data)));
    auto checkRange = [&](int32_t start, int32_t end,
                          int32_t expectedFrom, int32_t expectedTotal) {
        LOG(INFO) << "start " << start << ", end " << end
                  << ", expectedFrom " << expectedFrom << ", expectedTotal " << expectedTotal;
        std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
        std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
        std::unique_ptr<StorageIter> iter;
        EXPECT_EQ(ResultCode::SUCCESSED, engine->range(s, e, iter));
        int num = 0;
        while (iter->valid()) {
            num++;
            auto key = *reinterpret_cast<const int32_t*>(iter->key().data());
            auto val = iter->val();
            EXPECT_EQ(expectedFrom, key);
            EXPECT_EQ(folly::stringPrintf("val_%d", expectedFrom), val);
            expectedFrom++;
            iter->next();
        }
        EXPECT_EQ(expectedTotal, num);
    };

    checkRange(10, 20, 10, 10);
    checkRange(1, 50, 10, 10);
    checkRange(15, 18, 15, 3);
    checkRange(15, 23, 15, 5);
    checkRange(1, 15, 10, 5);
}

TEST(RocksdbEngineTest, PrefixTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_test.XXXXXX");
    std::unique_ptr<RocksdbEngine> engine = std::make_unique<RocksdbEngine>(0, rootPath.path());
    LOG(INFO) << "Write data in batch and scan them...";
    std::vector<KV> data;
    for (int32_t i = 0; i < 10;  i++) {
        data.emplace_back(folly::stringPrintf("a_%d", i),
                          folly::stringPrintf("val_%d", i));
    }
    for (int32_t i = 10; i < 15;  i++) {
        data.emplace_back(folly::stringPrintf("b_%d", i),
                          folly::stringPrintf("val_%d", i));
    }
    for (int32_t i = 20; i < 40;  i++) {
        data.emplace_back(folly::stringPrintf("c_%d", i),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(ResultCode::SUCCESSED, engine->multiPut(std::move(data)));
    auto checkPrefix = [&](const std::string& prefix,
                          int32_t expectedFrom, int32_t expectedTotal) {
        LOG(INFO) << "prefix " << prefix
                  << ", expectedFrom " << expectedFrom << ", expectedTotal " << expectedTotal;
        std::unique_ptr<StorageIter> iter;
        EXPECT_EQ(ResultCode::SUCCESSED, engine->prefix(prefix, iter));
        int num = 0;
        while (iter->valid()) {
            num++;
            auto key = iter->key();
            auto val = iter->val();
            EXPECT_EQ(folly::stringPrintf("%s_%d", prefix.c_str(), expectedFrom), key);
            EXPECT_EQ(folly::stringPrintf("val_%d", expectedFrom), val);
            expectedFrom++;
            iter->next();
        }
        EXPECT_EQ(expectedTotal, num);
    };
    checkPrefix("a", 0, 10);
    checkPrefix("b", 10, 5);
    checkPrefix("c", 20, 20);
}

}  // namespace storage
}  // namespace vesoft

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

