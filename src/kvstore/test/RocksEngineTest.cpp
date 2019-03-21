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
#include "kvstore/RocksEngine.h"

namespace nebula {
namespace kvstore {

TEST(RocksEngineTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_test.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, rootPath.path());
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->put("key", "val"));
    auto res = engine->get("key");
    EXPECT_TRUE(ok(res));
    EXPECT_EQ(value(res), "val");
}


TEST(RocksEngineTest, RangeTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_test.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, rootPath.path());
    std::vector<KV> data;
    for (int32_t i = 10; i < 20;  i++) {
        data.emplace_back(std::string(reinterpret_cast<const char*>(&i),
                                      sizeof(int32_t)),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->multiPut(std::move(data)));

    auto checkRange = [&](int32_t start, int32_t end,
                          int32_t expectedFrom, int32_t expectedTotal) {
        VLOG(1) << "start = " << start << ", end = " << end
                << ", expectedFrom = " << expectedFrom
                << ", expectedTotal = " << expectedTotal;
        std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
        std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
        auto res = engine->range(s, e);
        EXPECT_TRUE(ok(res));
        std::unique_ptr<KVIterator> iter = value(std::move(res));
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


TEST(RocksEngineTest, PrefixTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_test.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, rootPath.path());
    VLOG(1) << "Write data in batch and scan them...";
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
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->multiPut(std::move(data)));

    auto checkPrefix = [&](const std::string& prefix,
                           int32_t expectedFrom,
                           int32_t expectedTotal) {
        VLOG(1) << "prefix = \"" << prefix
                << "\", expectedFrom = " << expectedFrom
                << ", expectedTotal = " << expectedTotal;

        auto res = engine->prefix(prefix);
        EXPECT_TRUE(ok(res));
        std::unique_ptr<KVIterator> iter = value(std::move(res));
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


TEST(RocksEngineTest, RemoveTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_test.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, rootPath.path());
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->put("key", "val"));
    auto res1 = engine->get("key");
    EXPECT_TRUE(ok(res1));
    EXPECT_EQ(value(std::move(res1)), "val");

    EXPECT_EQ(ResultCode::SUCCEEDED, engine->remove("key"));
    auto res2 = engine->get("key");
    ASSERT_FALSE(ok(res2));
    EXPECT_EQ(ResultCode::ERR_KEY_NOT_FOUND, error(res2));
}


TEST(RocksEngineTest, RemoveRangeTest) {
    fs::TempDir rootPath("/tmp/rocksdb_remove_range_test.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, rootPath.path());
    for (int32_t i = 0; i < 100; i++) {
        auto key = std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t));
        EXPECT_EQ(ResultCode::SUCCEEDED,
                  engine->put(key, folly::stringPrintf("%d_val", i)));
        auto res = engine->get(key);
        EXPECT_TRUE(ok(res));
        EXPECT_EQ(value(std::move(res)), folly::stringPrintf("%d_val", i));
    }

    {
        int32_t s = 0;
        int32_t e = 50;
        EXPECT_EQ(ResultCode::SUCCEEDED,
                  engine->removeRange(
                    std::string(reinterpret_cast<const char*>(&s), sizeof(int32_t)),
                    std::string(reinterpret_cast<const char*>(&e), sizeof(int32_t))));
    }

    {
        int32_t s = 0;
        int32_t e = 100;
        std::string start(reinterpret_cast<const char*>(&s), sizeof(int32_t));
        std::string end(reinterpret_cast<const char*>(&e), sizeof(int32_t));

        auto res = engine->range(start, end);
        EXPECT_TRUE(ok(res));
        std::unique_ptr<KVIterator> iter = value(std::move(res));
        int num = 0;
        int expectedFrom = 50;
        while (iter->valid()) {
            num++;
            auto key = *reinterpret_cast<const int32_t*>(iter->key().data());
            auto val = iter->val();
            EXPECT_EQ(expectedFrom, key);
            EXPECT_EQ(folly::stringPrintf("%d_val", expectedFrom), val);
            expectedFrom++;
            iter->next();
        }
        EXPECT_EQ(50, num);
    }
}


TEST(RocksEngineTest, OptionTest) {
    fs::TempDir rootPath("/tmp/rocksdb_option_test.XXXXXX");
    std::unique_ptr<RocksEngine> engine = std::make_unique<RocksEngine>(0, rootPath.path());
    EXPECT_EQ(ResultCode::SUCCEEDED,
              engine->setOption("disable_auto_compactions", "true"));
    EXPECT_EQ(ResultCode::ERR_INVALID_ARGUMENT,
              engine->setOption("disable_auto_compactions_", "true"));
    EXPECT_EQ(ResultCode::ERR_INVALID_ARGUMENT,
              engine->setOption("disable_auto_compactions", "bad_value"));
    EXPECT_EQ(ResultCode::SUCCEEDED,
              engine->setDBOption("max_background_compactions", "2"));
    EXPECT_EQ(ResultCode::ERR_INVALID_ARGUMENT,
              engine->setDBOption("max_background_compactions_", "2"));
    EXPECT_EQ(ResultCode::SUCCEEDED,
              engine->setDBOption("max_background_compactions", "2_"));
    EXPECT_EQ(ResultCode::ERR_INVALID_ARGUMENT,
            engine->setDBOption("max_background_compactions", "bad_value"));
}

TEST(RocksEngineTest, CompactTest) {
    fs::TempDir rootPath("/tmp/rocksdb_compact_test.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, rootPath.path());
    std::vector<KV> data;
    for (int32_t i = 2; i < 8;  i++) {
        data.emplace_back(folly::stringPrintf("key_%d", i),
                          folly::stringPrintf("value_%d", i));
    }
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->multiPut(std::move(data)));
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->compactAll());
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

