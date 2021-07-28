/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <folly/lang/Bits.h>
#include "kvstore/RocksEngine.h"
#include "kvstore/RocksEngineConfig.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace kvstore {

const int32_t kDefaultVIdLen = 8;

TEST(RocksEngineTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_SimpleTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put("key", "val"));
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &val));
    EXPECT_EQ("val", val);
}


TEST(RocksEngineTest, RangeTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_RangeTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    std::vector<KV> data;
    for (int32_t i = 10; i < 20;  i++) {
        data.emplace_back(std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t)),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));

    auto checkRange = [&](int32_t start,
                          int32_t end,
                          int32_t expectedFrom,
                          int32_t expectedTotal) {
        VLOG(1) << "start " << start
                << ", end " << end
                << ", expectedFrom " << expectedFrom
                << ", expectedTotal " << expectedTotal;
        std::string s(reinterpret_cast<const char*>(&start), sizeof(int32_t));
        std::string e(reinterpret_cast<const char*>(&end), sizeof(int32_t));
        std::unique_ptr<KVIterator> iter;
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(s, e, &iter));
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
    fs::TempDir rootPath("/tmp/rocksdb_engine_PrefixTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
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
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));

    auto checkPrefix = [&](const std::string& prefix,
                           int32_t expectedFrom,
                           int32_t expectedTotal) {
        VLOG(1) << "prefix " << prefix
                << ", expectedFrom " << expectedFrom
                << ", expectedTotal " << expectedTotal;

        std::unique_ptr<KVIterator> iter;
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->prefix(prefix, &iter));
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
    fs::TempDir rootPath("/tmp/rocksdb_engine_RemoveTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put("key", "val"));
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &val));
    EXPECT_EQ("val", val);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->remove("key"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, engine->get("key", &val));
}


TEST(RocksEngineTest, RemoveRangeTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_RemoveRangeTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    for (int32_t i = 0; i < 100; i++) {
        std::string key(reinterpret_cast<const char*>(&i), sizeof(int32_t));
        std::string value(folly::stringPrintf("%d_val", i));
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put(key, value));
        std::string val;
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get(key, &val));
        EXPECT_EQ(value, val);
    }
    {
        int32_t s = 0, e = 50;
        EXPECT_EQ(
            nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->removeRange(
                std::string(reinterpret_cast<const char*>(&s), sizeof(int32_t)),
                std::string(reinterpret_cast<const char*>(&e), sizeof(int32_t))));
    }
    {
        int32_t s = 0, e = 100;
        std::unique_ptr<KVIterator> iter;
        std::string start(reinterpret_cast<const char*>(&s), sizeof(int32_t));
        std::string end(reinterpret_cast<const char*>(&e), sizeof(int32_t));
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(start, end, &iter));
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
    fs::TempDir rootPath("/tmp/rocksdb_engine_OptionTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->setOption("disable_auto_compactions", "true"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
              engine->setOption("disable_auto_compactions_", "true"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
              engine->setOption("disable_auto_compactions", "bad_value"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->setDBOption("max_background_compactions", "2"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
              engine->setDBOption("max_background_compactions_", "2"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->setDBOption("max_background_compactions", "2_"));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
              engine->setDBOption("max_background_compactions", "bad_value"));
}


TEST(RocksEngineTest, CompactTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_CompactTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    std::vector<KV> data;
    for (int32_t i = 2; i < 8;  i++) {
        data.emplace_back(folly::stringPrintf("key_%d", i),
                          folly::stringPrintf("value_%d", i));
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->compact());
}

TEST(RocksEngineTest, IngestTest) {
    rocksdb::Options options;
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    fs::TempDir rootPath("/tmp/rocksdb_engine_IngestTest.XXXXXX");
    auto file = folly::stringPrintf("%s/%s", rootPath.path(), "data.sst");
    auto stauts = writer.Open(file);
    ASSERT_TRUE(stauts.ok());

    stauts = writer.Put("key", "value");
    ASSERT_TRUE(stauts.ok());
    stauts = writer.Put("key_empty", "");
    ASSERT_TRUE(stauts.ok());
    writer.Finish();

    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    std::vector<std::string> files = {file};
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->ingest(files));

    std::string result;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &result));
    EXPECT_EQ("value", result);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key_empty", &result));
    EXPECT_EQ("", result);
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, engine->get("key_not_exist", &result));
}

TEST(RocksEngineTest, BackupRestoreTable) {
    rocksdb::Options options;
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    fs::TempDir rootPath("/tmp/rocksdb_engine_backuptable.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());

    std::vector<KV> data;
    for (int32_t i = 0; i < 10; i++) {
        data.emplace_back(folly::stringPrintf("part_%d", i),
                          folly::stringPrintf("val_%d", i));
        data.emplace_back(folly::stringPrintf("tags_%d", i),
                          folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));

    std::vector<std::string> sst_files;
    std::string partPrefix = "part_";
    std::string tagsPrefix = "tags_";
    auto parts = engine->backupTable("backup_test", partPrefix, nullptr);
    EXPECT_TRUE(ok(parts));
    sst_files.emplace_back(value(parts));
    auto tags = engine->backupTable("backup_test", tagsPrefix, [](const folly::StringPiece& key) {
        auto i = key.subpiece(5, key.size());
        if (folly::to<int>(i) % 2 == 0) {
            return true;
        }
        return false;
    });
    EXPECT_TRUE(ok(tags));
    sst_files.emplace_back(value(tags));

    fs::TempDir restoreRootPath("/tmp/rocksdb_engine_restoretable.XXXXXX");
    auto restore_engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, restoreRootPath.path());
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, restore_engine->ingest(sst_files));

    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, restore_engine->prefix(partPrefix, &iter));
    int index = 0;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        EXPECT_EQ(folly::stringPrintf("%s%d", partPrefix.c_str(), index), key);
        EXPECT_EQ(folly::stringPrintf("val_%d", index), val);
        iter->next();
        index++;
    }
    EXPECT_EQ(index, 10);

    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, restore_engine->prefix(tagsPrefix, &iter));
    index = 1;
    int num = 0;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        EXPECT_EQ(folly::stringPrintf("%s%d", tagsPrefix.c_str(), index), key);
        EXPECT_EQ(folly::stringPrintf("val_%d", index), val);
        iter->next();
        index += 2;
        num++;
    }
    EXPECT_EQ(num, 5);
}

TEST(RocksEngineTest, BackupRestoreWithoutData) {
    fs::TempDir dataPath("/tmp/rocks_engine_test_data_path.XXXXXX");
    fs::TempDir rocksdbWalPath("/tmp/rocks_engine_test_rocksdb_wal_path.XXXXXX");
    fs::TempDir backupPath("/tmp/rocks_engine_test_backup_path.XXXXXX");
    FLAGS_rocksdb_table_format = "PlainTable";
    FLAGS_rocksdb_wal_dir = rocksdbWalPath.path();
    FLAGS_rocksdb_backup_dir = backupPath.path();

    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, dataPath.path());

    LOG(INFO) << "Stop the engine and remove data";
    // release the engine and mock machine reboot by removing the data
    engine.reset();
    CHECK(fs::FileUtils::remove(dataPath.path(), true));

    LOG(INFO) << "Start recover";
    // reopen the engine, and it will try to restore from the previous backup
    engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, dataPath.path());

    FLAGS_rocksdb_table_format = "BlockBasedTable";
    FLAGS_rocksdb_wal_dir = "";
    FLAGS_rocksdb_backup_dir = "";
}

TEST(RocksEngineTest, BackupRestoreWithData) {
    fs::TempDir dataPath("/tmp/rocks_engine_test_data_path.XXXXXX");
    fs::TempDir rocksdbWalPath("/tmp/rocks_engine_test_rocksdb_wal_path.XXXXXX");
    fs::TempDir backupPath("/tmp/rocks_engine_test_backup_path.XXXXXX");
    FLAGS_rocksdb_table_format = "PlainTable";
    FLAGS_rocksdb_wal_dir = rocksdbWalPath.path();
    FLAGS_rocksdb_backup_dir = backupPath.path();

    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, dataPath.path());
    PartitionID partId = 1;

    auto checkData = [&] {
        std::string prefix = NebulaKeyUtils::vertexPrefix(kDefaultVIdLen, partId, "vertex");
        std::unique_ptr<KVIterator> iter;
        auto code = engine->prefix(prefix, &iter);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        int32_t num = 0;
        while (iter->valid()) {
            num++;
            iter->next();
        }
        EXPECT_EQ(num, 10);

        std::string value;
        code = engine->get(NebulaKeyUtils::systemCommitKey(partId), &value);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        EXPECT_EQ("123", value);
    };

    LOG(INFO) << "Write some data";
    std::vector<KV> data;
    for (auto tagId = 0; tagId < 10; tagId++) {
        data.emplace_back(NebulaKeyUtils::vertexKey(kDefaultVIdLen, partId, "vertex", tagId),
                          folly::stringPrintf("val_%d", tagId));
    }
    data.emplace_back(NebulaKeyUtils::systemCommitKey(partId), "123");
    engine->multiPut(std::move(data));

    checkData();
    LOG(INFO) << "Stop the engine and remove data";
    // release the engine and mock machine reboot by removing the data
    engine.reset();
    CHECK(fs::FileUtils::remove(dataPath.path(), true));

    LOG(INFO) << "Start recover";
    // reopen the engine, and it will try to restore from the previous backup
    engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, dataPath.path());
    checkData();

    FLAGS_rocksdb_table_format = "BlockBasedTable";
    FLAGS_rocksdb_wal_dir = "";
    FLAGS_rocksdb_backup_dir = "";
}

TEST(RocksEngineTest, VertexBloomFilterTest) {
    FLAGS_enable_rocksdb_statistics = true;
    fs::TempDir rootPath("/tmp/rocksdb_engine_VertexBloomFilterTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    PartitionID partId = 1;
    VertexID vId = "vertex";

    auto writeVertex = [&](TagID tagId) {
        std::vector<KV> data;
        data.emplace_back(NebulaKeyUtils::vertexKey(kDefaultVIdLen, partId, vId, tagId),
                          folly::stringPrintf("val_%d", tagId));
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
    };

    auto readVertex = [&](TagID tagId) {
        auto key = NebulaKeyUtils::vertexKey(kDefaultVIdLen, partId, vId, tagId);
        std::string val;
        auto ret = engine->get(key, &val);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            EXPECT_EQ(folly::stringPrintf("val_%d", tagId), val);
        } else {
            EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, ret);
        }
    };

    auto statistics = kvstore::getDBStatistics();

    // write initial vertex
    writeVertex(0);

    // read data while in memtable
    readVertex(0);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    readVertex(1);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);

    // flush to sst, read again
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
    readVertex(0);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    // read not exists data, whole key bloom filter will be useful
    readVertex(1);
    EXPECT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);

    FLAGS_enable_rocksdb_statistics = false;
}


TEST(RocksEngineTest, EdgeBloomFilterTest) {
    FLAGS_enable_rocksdb_statistics = true;
    fs::TempDir rootPath("/tmp/rocksdb_engine_EdgeBloomFilterTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
    PartitionID partId = 1;
    VertexID vId = "vertex";
    auto writeEdge = [&](EdgeType edgeType) {
        std::vector<KV> data;
        data.emplace_back(NebulaKeyUtils::edgeKey(kDefaultVIdLen, partId, vId, edgeType, 0, vId),
                          folly::stringPrintf("val_%d", edgeType));
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
    };

    auto readEdge = [&](EdgeType edgeType) {
        auto key = NebulaKeyUtils::edgeKey(kDefaultVIdLen, partId, vId, edgeType, 0, vId);
        std::string val;
        auto ret = engine->get(key, &val);
        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            EXPECT_EQ(folly::stringPrintf("val_%d", edgeType), val);
        } else {
            EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, ret);
        }
    };

    auto statistics = kvstore::getDBStatistics();
    statistics->getAndResetTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL);

    // write initial vertex
    writeEdge(0);

    // read data while in memtable
    readEdge(0);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    readEdge(1);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);

    // flush to sst, read again
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
    readEdge(0);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    // read not exists data, whole key bloom filter will be useful
    readEdge(1);
    EXPECT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);

    FLAGS_enable_rocksdb_statistics = false;
}

class RocksEnginePrefixTest
    : public ::testing::TestWithParam<std::tuple<bool, std::string, int32_t>> {
public:
    void SetUp() override {
        auto param = GetParam();
        FLAGS_enable_rocksdb_prefix_filtering = std::get<0>(param);
        FLAGS_rocksdb_table_format = std::get<1>(param);
        if (FLAGS_rocksdb_table_format == "PlainTable") {
            FLAGS_rocksdb_plain_table_prefix_length = std::get<2>(param);
        }
    }

    void TearDown() override {}
};

TEST_P(RocksEnginePrefixTest, PrefixTest) {
    fs::TempDir rootPath("/tmp/rocksdb_engine_PrefixExtractorTest.XXXXXX");
    auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());

    PartitionID partId = 1;

    std::vector<KV> data;
    for (auto tagId = 0; tagId < 10; tagId++) {
        data.emplace_back(NebulaKeyUtils::vertexKey(kDefaultVIdLen, partId, "vertex", tagId),
                          folly::stringPrintf("val_%d", tagId));
    }
    data.emplace_back(NebulaKeyUtils::systemCommitKey(partId), "123");
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));

    {
        std::string prefix = NebulaKeyUtils::vertexPrefix(kDefaultVIdLen, partId, "vertex");
        std::unique_ptr<KVIterator> iter;
        auto code = engine->prefix(prefix, &iter);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        int32_t num = 0;
        while (iter->valid()) {
            num++;
            iter->next();
        }
        EXPECT_EQ(num, 10);
    }
    {
        std::string prefix = NebulaKeyUtils::vertexPrefix(partId);
        std::unique_ptr<KVIterator> iter;
        auto code = engine->prefix(prefix, &iter);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        int32_t num = 0;
        while (iter->valid()) {
            num++;
            iter->next();
        }
        EXPECT_EQ(num, 10);
    }
    {
        std::string value;
        auto code = engine->get(NebulaKeyUtils::systemCommitKey(partId), &value);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        EXPECT_EQ("123", value);
    }
}

INSTANTIATE_TEST_CASE_P(
    PrefixExtractor_TableFormat_PlainTablePrefixSize,
    RocksEnginePrefixTest,
    ::testing::Values(std::make_tuple(false, "BlockBasedTable", 0),
                      std::make_tuple(true, "BlockBasedTable", 0),
                      // PlainTable will always enable prefix extractor
                      std::make_tuple(true, "PlainTable", sizeof(PartitionID)),
                      std::make_tuple(true, "PlainTable", sizeof(PartitionID) + kDefaultVIdLen)));

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

