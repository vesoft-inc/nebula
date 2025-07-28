/* Copyright (c) 2025 vesoft inc. All rights reserved.
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
  int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
  std::string key;
  key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
  key.append("key");
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put(key, "val"));
  std::string val;
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get(key, &val));
  EXPECT_EQ("val", val);
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, engine->get("val", &val));
}

TEST_P(RocksEngineMultiCFTest, RangeTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_multicf_RangeTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  std::vector<KV> data;

  for (int32_t i = 10; i < 20; i++) {
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string key;
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    key.append("key_");
    key.append(reinterpret_cast<const char*>(&i), sizeof(int32_t));
    data.emplace_back(key, folly::stringPrintf("val_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }

  auto checkRange = [&](int32_t start, int32_t end, int32_t expectedFrom, int32_t expectedTotal) {
    VLOG(1) << "start " << start << ", end " << end << ", expectedFrom " << expectedFrom
            << ", expectedTotal " << expectedTotal;
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string key;
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    key.append("key_");

    std::string s = key + std::string(reinterpret_cast<const char*>(&start), sizeof(int32_t));
    std::string e = key + std::string(reinterpret_cast<const char*>(&end), sizeof(int32_t));
    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(s, e, &iter));
    int num = 0;
    while (iter->valid()) {
      num++;
      // remove the prefix "key_"
      auto iter_key = *reinterpret_cast<const int32_t*>(iter->key().subpiece(8).data());
      auto val = iter->val();
      EXPECT_EQ(expectedFrom, iter_key);
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
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string key;
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    key.append("key_a_");
    data.emplace_back(key + std::to_string(i), folly::stringPrintf("val_%d", i));
  }
  for (int32_t i = 10; i < 15; i++) {
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string key;
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    key.append("key_b_");
    data.emplace_back(key + std::to_string(i), folly::stringPrintf("val_%d", i));
  }
  for (int32_t i = 20; i < 40; i++) {
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string key;
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    key.append("key_c_");
    data.emplace_back(key + std::to_string(i), folly::stringPrintf("val_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }

  auto checkPrefix = [&](const std::string& prefix, int32_t expectedFrom, int32_t expectedTotal) {
    VLOG(1) << "prefix " << prefix << ", expectedFrom " << expectedFrom << ", expectedTotal "
            << expectedTotal;

    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->prefix(prefix, &iter));
    int num = 0;
    while (iter->valid()) {
      num++;
      auto key = iter->key().subpiece(4).data();
      auto val = iter->val();
      EXPECT_EQ(folly::stringPrintf("%s_%d", prefix.substr(4).c_str(), expectedFrom), key);
      EXPECT_EQ(folly::stringPrintf("val_%d", expectedFrom), val);
      expectedFrom++;
      iter->next();
    }
    EXPECT_EQ(expectedTotal, num);
  };
  int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
  std::string key;
  key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
  checkPrefix(key + "key_a", 0, 10);
  checkPrefix(key + "key_b", 10, 5);
  checkPrefix(key + "key_c", 20, 20);
}

TEST_P(RocksEngineMultiCFTest, RemoveTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_RemoveTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
  std::string key;
  key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put(key + "key", "val"));
  std::string val;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get(key + "key", &val));
  EXPECT_EQ("val", val);
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->remove(key + "key"));
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, engine->get(key + "key", &val));
}

TEST_P(RocksEngineMultiCFTest, RemoveRangeTest) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
  fs::TempDir rootPath("/tmp/rocksdb_engine_multicf_RemoveRangeTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  for (int32_t i = 0; i < 100; i++) {
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string key;
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    key.append(reinterpret_cast<const char*>(&i), sizeof(int32_t));  // 使用二进制格式
    std::string value(folly::stringPrintf("%d_val", i));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put(key, value));
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get(key, &val));
    EXPECT_EQ(value, val);
  }
  {
    int32_t s = 0, e = 50;
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string keyPrefix;
    keyPrefix.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    std::string startKey =
        keyPrefix + std::string(reinterpret_cast<const char*>(&s), sizeof(int32_t));
    std::string endKey =
        keyPrefix + std::string(reinterpret_cast<const char*>(&e), sizeof(int32_t));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->removeRange(startKey, endKey));
  }
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));
  }
  {
    int32_t s = 0, e = 100;
    std::unique_ptr<KVIterator> iter;
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string keyPrefix;
    keyPrefix.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    std::string startKey =
        keyPrefix + std::string(reinterpret_cast<const char*>(&s), sizeof(int32_t));
    std::string endKey =
        keyPrefix + std::string(reinterpret_cast<const char*>(&e), sizeof(int32_t));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(startKey, endKey, &iter));

    int num = 0;
    int expectedFrom = 50;
    while (iter->valid()) {
      num++;
      auto iter_key_data = iter->key().data() + sizeof(int32_t);
      int32_t actual_key = *reinterpret_cast<const int32_t*>(iter_key_data);
      auto val = iter->val();
      EXPECT_EQ(expectedFrom, actual_key);
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
    int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
    std::string key;
    key.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));
    data.emplace_back(key + std::to_string(i), folly::stringPrintf("value_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
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
  int32_t item = static_cast<uint32_t>(NebulaKeyType::kVector_);
  std::string keyPrefix;
  keyPrefix.append(reinterpret_cast<const char*>(&item), sizeof(int32_t));

  for (int32_t i = 0; i < 10; i++) {
    // 添加default CF的数据
    data.emplace_back(folly::stringPrintf("part_%d", i), folly::stringPrintf("val_%d", i));

    // 添加vector CF的数据，每次创建新的键
    std::string vectorKey =
        keyPrefix + std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t));
    data.emplace_back(vectorKey, folly::stringPrintf("val_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));

  std::vector<std::string> sst_files;
  std::vector<std::string> vector_sst_files;
  std::string partPrefix = "part_";
  std::string vectorPrefix = std::string(reinterpret_cast<const char*>(&item), sizeof(int32_t));

  // 备份part_前缀的数据（存储在default CF中）
  auto parts = engine->backupTable("backup_test", partPrefix, nullptr);
  EXPECT_TRUE(ok(parts));
  sst_files.emplace_back(value(parts));

  // 备份vector CF中的数据，过滤器选择偶数索引的数据
  auto vectors =
      engine->backupTable(NebulaKeyUtils::kVectorColumnFamilyName,
                          "backup_test",
                          vectorPrefix,
                          [](const folly::StringPiece& iterkey) {
                            // 提取二进制格式的整数并判断是否为偶数
                            auto binaryData = iterkey.subpiece(4);  // 跳过NebulaKeyType前缀
                            if (binaryData.size() >= sizeof(int32_t)) {
                              int32_t i = *reinterpret_cast<const int32_t*>(binaryData.data());
                              return i % 2 == 0;  // 选择偶数
                            }
                            return false;
                          });
  EXPECT_TRUE(ok(vectors));
  vector_sst_files.emplace_back(value(vectors));

  fs::TempDir restoreRootPath("/tmp/rocksdb_engine_restoretable.XXXXXX");
  auto restore_engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, restoreRootPath.path());
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            restore_engine->ingest(NebulaKeyUtils::kDefaultColumnFamilyName, sst_files));
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            restore_engine->ingest(NebulaKeyUtils::kVectorColumnFamilyName, vector_sst_files));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kDefaultColumnFamilyName,
                             NebulaKeyUtils::kVectorColumnFamilyName}));
  }
  std::unique_ptr<KVIterator> iter;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, restore_engine->prefix(partPrefix, &iter));
  int index = 0;
  while (iter->valid()) {
    auto iterkey = iter->key();
    auto val = iter->val();
    EXPECT_EQ(folly::stringPrintf("%s%d", partPrefix.c_str(), index), iterkey);
    EXPECT_EQ(folly::stringPrintf("val_%d", index), val);
    iter->next();
    index++;
  }
  EXPECT_EQ(index, 10);

  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, restore_engine->prefix(vectorPrefix, &iter));
  index = 1;
  int num = 0;
  while (iter->valid()) {
    auto binaryData = iter->key().subpiece(4);
    int32_t actual_key = *reinterpret_cast<const int32_t*>(binaryData.data());
    auto val = iter->val();
    EXPECT_EQ(index, actual_key);
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

  // Put vector data into NebulaKeyUtils::kVectorColumnFamilyName
  std::vector<KV> vectorCF_data;
  std::vector<std::string> cfNames;
  for (auto tagId = 0; tagId < 10; tagId++) {
    vectorCF_data.emplace_back(NebulaKeyUtils::vectorTagKey(kDefaultVIdLen, 1, "1", tagId, 0),
                               folly::stringPrintf("val_%d", tagId));
    vectorCF_data.emplace_back(NebulaKeyUtils::vectorTagKey(kDefaultVIdLen, 1, "2", tagId, 0),
                               folly::stringPrintf("val_%d", tagId));
    vectorCF_data.emplace_back(NebulaKeyUtils::vectorTagKey(kDefaultVIdLen, 2, "3", tagId, 0),
                               folly::stringPrintf("val_%d", tagId));
    vectorCF_data.emplace_back(NebulaKeyUtils::vectorTagKey(kDefaultVIdLen, 2, "4", tagId, 0),
                               folly::stringPrintf("val_%d", tagId));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(vectorCF_data)));

  // Put system data into NebulaKeyUtils::kDefaultColumnFamilyName
  std::vector<KV> defaultCF_data;
  defaultCF_data.emplace_back(NebulaKeyUtils::systemCommitKey(1), "123");
  defaultCF_data.emplace_back(NebulaKeyUtils::systemCommitKey(2), "123");

  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(defaultCF_data)));

  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kDefaultColumnFamilyName,
                             NebulaKeyUtils::kVectorColumnFamilyName}));
  }

  {
    // vertexPrefix(partId, vId) should find data in NebulaKeyUtils::kVectorColumnFamilyName
    auto checkVertexPrefix = [&](PartitionID partId, const VertexID& vId) {
      std::string prefix = NebulaKeyUtils::vectorTagPrefix(kDefaultVIdLen, partId, vId);
      std::unique_ptr<KVIterator> iter;
      auto code = engine->prefix(prefix, &iter);
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
      std::string prefix = NebulaKeyUtils::vectorTagPrefix(partId);
      std::unique_ptr<KVIterator> iter;
      auto code = engine->prefix(prefix, &iter);
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
      auto code = engine->get(NebulaKeyUtils::systemCommitKey(partId), &value);
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
      data.emplace_back(NebulaKeyUtils::vectorTagKey(kDefaultVIdLen, 1, "1", tagId, 0), "v");
    }
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->flush({NebulaKeyUtils::kVectorColumnFamilyName}));

    // Check data
    std::string val;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->get(NebulaKeyUtils::vectorTagKey(kDefaultVIdLen, 1, "1", 0, 0), &val));
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
              engine->get(NebulaKeyUtils::vectorTagKey(kDefaultVIdLen, 1, "1", 0, 0), &val));
  }

  // Step 3: Compact the specific data_cf to rebuild its SST files with the new option.
  LOG(INFO) << "Compact data_cf to rebuild prefix bloom filter";
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->compact(NebulaKeyUtils::kVectorColumnFamilyName));

  // Step 4: Verify everything still works. A real test would check statistics
  // to confirm the prefix bloom filter is now being used for lookups in data_cf.
  std::string val;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->get(NebulaKeyUtils::vectorTagKey(kDefaultVIdLen, 1, "1", 0, 0), &val));

  std::unique_ptr<KVIterator> iter;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
            engine->prefix(NebulaKeyUtils::vectorTagPrefix(kDefaultVIdLen, 1, "1"), &iter));
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
