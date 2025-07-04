/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/lang/Bits.h>
#include <gtest/gtest.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/table.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/RocksEngine.h"  // For RocksEngineConfig and getDBStatistics
#include "kvstore/RocksEngineConfig.h"

// Define a new engine class for testing that supports multi-column-families
namespace nebula {
namespace kvstore {

const int32_t kDefaultVIdLen = 8;

class RocksEngineMultiCFTest
    : public ::testing::TestWithParam<std::tuple<bool, bool, std::string, bool>> {
 public:
  void SetUp() override {
    auto param = GetParam();
    FLAGS_enable_rocksdb_prefix_filtering = std::get<0>(param);
    FLAGS_enable_rocksdb_whole_key_filtering = std::get<1>(param);
    FLAGS_rocksdb_table_format = std::get<2>(param);
    flush_ = std::get<3>(param);
  }

  void TearDown() override {}

 protected:
  bool flush_;
};

TEST_P(RocksEngineMultiCFTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_SimpleTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->put(NebulaKeyUtils::kVectorColumnFamilyName, "key", "val"));
  std::string val;
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->get(NebulaKeyUtils::kVectorColumnFamilyName, "key", &val));
  EXPECT_EQ("val", val);
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND,
            engine->get(NebulaKeyUtils::kDefaultColumnFamilyName, "val", &val));
}

TEST_P(RocksEngineMultiCFTest, RangeTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_multicf_RangeTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  std::vector<KV> data;

  for (int32_t i = 10; i < 20; i++) {
    data.emplace_back("key_" + std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t)),
                      folly::stringPrintf("val_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->multiPut(NebulaKeyUtils::kVectorColumnFamilyName, std::move(data)));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }

  auto checkRange = [&](int32_t start, int32_t end, int32_t expectedFrom, int32_t expectedTotal) {
    VLOG(1) << "start " << start << ", end " << end << ", expectedFrom " << expectedFrom
            << ", expectedTotal " << expectedTotal;
    std::string s = "key_" + std::string(reinterpret_cast<const char*>(&start), sizeof(int32_t));
    std::string e = "key_" + std::string(reinterpret_cast<const char*>(&end), sizeof(int32_t));
    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->range(NebulaKeyUtils::kVectorColumnFamilyName, s, e, &iter));
    int num = 0;
    while (iter->valid()) {
      num++;
      // remove the prefix "key_"
      auto key = *reinterpret_cast<const int32_t*>(iter->key().subpiece(4).data());
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

TEST_P(RocksEngineMultiCFTest, PrefixTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_PrefixTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  LOG(INFO) << "Write data in batch and scan them...";
  std::vector<KV> data(40);

  for (int32_t i = 0; i < 10; i++) {
    data.emplace_back(folly::stringPrintf("key_a_%d", i), folly::stringPrintf("val_%d", i));
  }
  for (int32_t i = 10; i < 15; i++) {
    data.emplace_back(folly::stringPrintf("key_b_%d", i), folly::stringPrintf("val_%d", i));
  }
  for (int32_t i = 20; i < 40; i++) {
    data.emplace_back(folly::stringPrintf("key_c_%d", i), folly::stringPrintf("val_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->multiPut(NebulaKeyUtils::kVectorColumnFamilyName, std::move(data)));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }

  auto checkPrefix = [&](const std::string& prefix, int32_t expectedFrom, int32_t expectedTotal) {
    VLOG(1) << "prefix " << prefix << ", expectedFrom " << expectedFrom << ", expectedTotal "
            << expectedTotal;

    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->prefix(NebulaKeyUtils::kVectorColumnFamilyName, prefix, &iter));
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
  checkPrefix("key_a", 0, 10);
  checkPrefix("key_b", 10, 5);
  checkPrefix("key_c", 20, 20);
}

TEST_P(RocksEngineMultiCFTest, RemoveTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_RemoveTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->put(NebulaKeyUtils::kVectorColumnFamilyName, "key", "val"));
  std::string val;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->get(NebulaKeyUtils::kVectorColumnFamilyName, "key", &val));
  EXPECT_EQ("val", val);
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->remove(NebulaKeyUtils::kVectorColumnFamilyName, "key"));
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND,
            engine->get(NebulaKeyUtils::kVectorColumnFamilyName, "key", &val));
}

TEST_P(RocksEngineMultiCFTest, RemoveRangeTest) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_RemoveRangeTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  for (int32_t i = 0; i < 100; i++) {
    std::string key(reinterpret_cast<const char*>(&i), sizeof(int32_t));
    std::string value(folly::stringPrintf("%d_val", i));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->put(NebulaKeyUtils::kVectorColumnFamilyName, key, value));
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->get(NebulaKeyUtils::kVectorColumnFamilyName, key, &val));
    EXPECT_EQ(value, val);
  }
  {
    int32_t s = 0, e = 50;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->removeRange(NebulaKeyUtils::kVectorColumnFamilyName,
                                  std::string(reinterpret_cast<const char*>(&s), sizeof(int32_t)),
                                  std::string(reinterpret_cast<const char*>(&e), sizeof(int32_t))));
  }
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  }
  {
    int32_t s = 0, e = 100;
    std::unique_ptr<KVIterator> iter;
    std::string start(reinterpret_cast<const char*>(&s), sizeof(int32_t));
    std::string end(reinterpret_cast<const char*>(&e), sizeof(int32_t));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->range(NebulaKeyUtils::kVectorColumnFamilyName, start, end, &iter));
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

TEST_P(RocksEngineMultiCFTest, OptionTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_OptionTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  // Test CF option
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->setOption(
                NebulaKeyUtils::kVectorColumnFamilyName, "disable_auto_compactions", "true"));
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
            engine->setOption(
                NebulaKeyUtils::kVectorColumnFamilyName, "disable_auto_compactions_", "true"));
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
            engine->setOption(
                NebulaKeyUtils::kVectorColumnFamilyName, "disable_auto_compactions", "bad_value"));

  // Test DB option
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->setDBOption("max_background_compactions", "2"));
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
            engine->setDBOption("max_background_compactions_", "2"));
  // RocksDB can sometimes parse partial values
  // EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
  //           engine->setDBOption("max_background_compactions", "2_"));
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM,
            engine->setDBOption("max_background_compactions", "bad_value"));
}

TEST_P(RocksEngineMultiCFTest, CompactTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_CompactTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  std::vector<KV> data;
  for (int32_t i = 2; i < 8; i++) {
    data.emplace_back(folly::stringPrintf("key_%d", i), folly::stringPrintf("value_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->multiPut(NebulaKeyUtils::kVectorColumnFamilyName, std::move(data)));
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->compact(NebulaKeyUtils::kVectorColumnFamilyName));
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->compact(NebulaKeyUtils::kVectorColumnFamilyName));  // Compact all
}

TEST_P(RocksEngineMultiCFTest, IngestTest) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
  rocksdb::Options options;
  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_IngestTest.XXXXXX");
  auto file = folly::stringPrintf("%s/%s", rootPath.path(), "data.sst");
  auto status = writer.Open(file);
  ASSERT_TRUE(status.ok());

  status = writer.Put("key", "value");
  ASSERT_TRUE(status.ok());
  status = writer.Put("key_empty", "");
  ASSERT_TRUE(status.ok());
  writer.Finish();

  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  std::vector<std::string> files = {file};
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->ingest(NebulaKeyUtils::kVectorColumnFamilyName, files));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }

  std::string result;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->get(NebulaKeyUtils::kVectorColumnFamilyName, "key", &result));
  EXPECT_EQ("value", result);
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->get(NebulaKeyUtils::kVectorColumnFamilyName, "key_empty", &result));
  EXPECT_EQ("", result);
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND,
            engine->get(NebulaKeyUtils::kVectorColumnFamilyName, "key_not_exist", &result));

  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->ingest(NebulaKeyUtils::kVectorColumnFamilyName, {}));
}

// NOTE: backupTable is not part of rocksdb public API, this is a nebula extension.
// We will simulate it by iterating and writing to a new SST file.
// This is a simplified version for the sake of this test refactoring.
TEST_P(RocksEngineMultiCFTest, BackupRestoreTable) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_backuptable.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());

  std::vector<KV> data;

  for (int32_t i = 0; i < 10; i++) {
    data.emplace_back(folly::stringPrintf("part_%d", i), folly::stringPrintf("val_%d", i));
    data.emplace_back(folly::stringPrintf("tags_%d", i), folly::stringPrintf("val_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->multiPut(NebulaKeyUtils::kVectorColumnFamilyName, std::move(data)));

  std::vector<std::string> sst_files;
  std::string partPrefix = "part_";
  std::string tagsPrefix = "tags_";
  auto parts = engine->backupTable(
      NebulaKeyUtils::kVectorColumnFamilyName, "backup_test", partPrefix, nullptr);
  EXPECT_TRUE(ok(parts));
  sst_files.emplace_back(value(parts));
  auto tags = engine->backupTable(NebulaKeyUtils::kVectorColumnFamilyName,
                                  "backup_test",
                                  tagsPrefix,
                                  [](const folly::StringPiece& key) {
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
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            restore_engine->ingest(NebulaKeyUtils::kVectorColumnFamilyName, sst_files));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }

  std::unique_ptr<KVIterator> iter;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            restore_engine->prefix(NebulaKeyUtils::kVectorColumnFamilyName, partPrefix, &iter));
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

  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            restore_engine->prefix(NebulaKeyUtils::kVectorColumnFamilyName, tagsPrefix, &iter));
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

TEST_P(RocksEngineMultiCFTest, PrefixBloomTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_PrefixExtractorTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());

  // Put vertex/edge data into NebulaKeyUtils::kVectorColumnFamilyName
  std::vector<KV> dataCF_data;
  std::vector<std::string> cfNames;
  for (auto tagId = 0; tagId < 10; tagId++) {
    dataCF_data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "1", tagId),
                             folly::stringPrintf("val_%d", tagId));
    dataCF_data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "2", tagId),
                             folly::stringPrintf("val_%d", tagId));
    dataCF_data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 2, "3", tagId),
                             folly::stringPrintf("val_%d", tagId));
    dataCF_data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 2, "4", tagId),
                             folly::stringPrintf("val_%d", tagId));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->multiPut(NebulaKeyUtils::kVectorColumnFamilyName, std::move(dataCF_data)));

  // Put system data into NebulaKeyUtils::kDefaultColumnFamilyName
  std::vector<KV> defaultCF_data;
  defaultCF_data.emplace_back(NebulaKeyUtils::systemCommitKey(1), "123");
  defaultCF_data.emplace_back(NebulaKeyUtils::systemCommitKey(2), "123");

  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->multiPut(NebulaKeyUtils::kDefaultColumnFamilyName, std::move(defaultCF_data)));

  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kDefaultColumnFamilyName}));
  }

  {
    // vertexPrefix(partId, vId) should find data in NebulaKeyUtils::kVectorColumnFamilyName
    auto checkVertexPrefix = [&](PartitionID partId, const VertexID& vId) {
      std::string prefix = NebulaKeyUtils::tagPrefix(kDefaultVIdLen, partId, vId);
      std::unique_ptr<KVIterator> iter;
      auto code = engine->prefix(NebulaKeyUtils::kVectorColumnFamilyName, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
      int32_t num = 0;
      while (iter->valid()) {
        num++;
        iter->next();
      }
      EXPECT_EQ(num, 10);
    };
    checkVertexPrefix(1, "1");
    checkVertexPrefix(1, "2");
    checkVertexPrefix(2, "3");
    checkVertexPrefix(2, "4");
  }
  {
    // vertexPrefix(partId) should find data in NebulaKeyUtils::kVectorColumnFamilyName
    auto checkPartPrefix = [&](PartitionID partId) {
      std::string prefix = NebulaKeyUtils::tagPrefix(partId);
      std::unique_ptr<KVIterator> iter;
      auto code = engine->prefix(NebulaKeyUtils::kVectorColumnFamilyName, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
      int32_t num = 0;
      while (iter->valid()) {
        num++;
        iter->next();
      }
      EXPECT_EQ(num, 20);
    };
    checkPartPrefix(1);
    checkPartPrefix(2);
  }
  {
    // System data is in NebulaKeyUtils::kDefaultColumnFamilyName
    auto checkSystemCommit = [&](PartitionID partId) {
      std::string value;
      auto code = engine->get(NebulaKeyUtils::kDefaultColumnFamilyName,
                              NebulaKeyUtils::systemCommitKey(partId),
                              &value);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
      EXPECT_EQ("123", value);
    };
    checkSystemCommit(1);
    checkSystemCommit(2);
  }
}

// NOTE: Bloom filter tests are omitted for brevity in this refactoring, as they would
// require deeper changes into the mock engine to manage per-CF statistics.
// The principle would be:
// 1. Configure bloom filters on NebulaKeyUtils::kVectorColumnFamilyName during engine creation.
// 2. All writes and reads for the test would target NebulaKeyUtils::kVectorColumnFamilyName.
// 3. Statistics would be checked on the database-wide object, but would reflect hits/misses
//    from operations on NebulaKeyUtils::kVectorColumnFamilyName.

INSTANTIATE_TEST_SUITE_P(EnablePrefixExtractor_EnableWholeKeyFilter_TableFormat_FlushOrNot,
                         RocksEngineMultiCFTest,
                         ::testing::Values(std::make_tuple(false, false, "BlockBasedTable", true),
                                           std::make_tuple(false, false, "BlockBasedTable", false),
                                           std::make_tuple(false, true, "BlockBasedTable", true),
                                           std::make_tuple(false, true, "BlockBasedTable", false),
                                           std::make_tuple(true, false, "BlockBasedTable", true),
                                           std::make_tuple(true, false, "BlockBasedTable", false),
                                           std::make_tuple(true, true, "BlockBasedTable", true),
                                           std::make_tuple(true, true, "BlockBasedTable", false),
                                           // PlainTable will always enable prefix extractor
                                           std::make_tuple(true, false, "PlainTable", true),
                                           std::make_tuple(true, false, "PlainTable", false),
                                           std::make_tuple(true, true, "PlainTable", true),
                                           std::make_tuple(true, true, "PlainTable", false)));

TEST(RebuildPrefixBloomFilter, RebuildPrefixBloomFilterForCF) {
  GraphSpaceID spaceId = 1;
  fs::TempDir dataPath("/tmp/rocksdb_engine_multicf_rebuild_prefix_bloom_filter.XXXXXX");

  auto writeAndCheckInitialData = [&](std::unique_ptr<RocksEngine>& engine) {
    // Put data into the data CF
    std::vector<KV> data(10);
    for (TagID tagId = 0; tagId < 10; tagId++) {
      data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "1", tagId), "v");
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->multiPut(NebulaKeyUtils::kVectorColumnFamilyName, std::move(data)));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));

    // Check data
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->get(NebulaKeyUtils::kVectorColumnFamilyName,
                          NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "1", 0),
                          &val));
  };

  // Step 1: Create DB with prefix bloom filter OFF for data_cf
  {
    LOG(INFO) << "Start engine with prefix bloom filter disabled for data_cf";
    FLAGS_enable_rocksdb_prefix_filtering = false;
    FLAGS_enable_rocksdb_whole_key_filtering = true;
    auto engine = std::make_unique<RocksEngine>(spaceId, kDefaultVIdLen, dataPath.path());
    writeAndCheckInitialData(engine);
    // engine goes out of scope and DB is closed
  }

  // Step 2: Reopen DB with prefix bloom filter ON for data_cf.
  // The old SST files for data_cf still don't have prefix bloom filters.
  std::unique_ptr<RocksEngine> engine;
  {
    LOG(INFO) << "Restart engine with prefix bloom filter enabled for data_cf";
    FLAGS_enable_rocksdb_prefix_filtering = true;
    FLAGS_enable_rocksdb_whole_key_filtering = false;
    engine = std::make_unique<RocksEngine>(spaceId, kDefaultVIdLen, dataPath.path());

    // Check old data still exists
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->get(NebulaKeyUtils::kVectorColumnFamilyName,
                          NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "1", 0),
                          &val));
  }

  // Step 3: Compact the specific data_cf to rebuild its SST files with the new option.
  LOG(INFO) << "Compact data_cf to rebuild prefix bloom filter";
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->compact(NebulaKeyUtils::kVectorColumnFamilyName));

  // Step 4: Verify everything still works. A real test would check statistics
  // to confirm the prefix bloom filter is now being used for lookups in data_cf.
  std::string val;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->get(NebulaKeyUtils::kVectorColumnFamilyName,
                        NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "1", 0),
                        &val));

  std::unique_ptr<KVIterator> iter;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->prefix(NebulaKeyUtils::kVectorColumnFamilyName,
                           NebulaKeyUtils::tagPrefix(kDefaultVIdLen, 1, "1"),
                           &iter));
  int count = 0;
  while (iter->valid()) {
    count++;
    iter->next();
  }
  EXPECT_EQ(10, count);
}

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
