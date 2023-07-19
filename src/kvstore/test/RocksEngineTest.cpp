/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/lang/Bits.h>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/table.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/RocksEngine.h"
#include "kvstore/RocksEngineConfig.h"

namespace nebula {
namespace kvstore {

const int32_t kDefaultVIdLen = 8;

class RocksEngineTest : public ::testing::TestWithParam<std::tuple<bool, bool, std::string, bool>> {
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

TEST_P(RocksEngineTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_SimpleTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put("key", "val"));
  std::string val;
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &val));
  EXPECT_EQ("val", val);
}

TEST_P(RocksEngineTest, RangeTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_RangeTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  std::vector<KV> data;
  for (int32_t i = 10; i < 20; i++) {
    data.emplace_back("key_" + std::string(reinterpret_cast<const char*>(&i), sizeof(int32_t)),
                      folly::stringPrintf("val_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  }

  auto checkRange = [&](int32_t start, int32_t end, int32_t expectedFrom, int32_t expectedTotal) {
    VLOG(1) << "start " << start << ", end " << end << ", expectedFrom " << expectedFrom
            << ", expectedTotal " << expectedTotal;
    std::string s = "key_" + std::string(reinterpret_cast<const char*>(&start), sizeof(int32_t));
    std::string e = "key_" + std::string(reinterpret_cast<const char*>(&end), sizeof(int32_t));
    std::unique_ptr<KVIterator> iter;
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->range(s, e, &iter));
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

TEST_P(RocksEngineTest, PrefixTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_PrefixTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  LOG(INFO) << "Write data in batch and scan them...";
  std::vector<KV> data;
  for (int32_t i = 0; i < 10; i++) {
    data.emplace_back(folly::stringPrintf("key_a_%d", i), folly::stringPrintf("val_%d", i));
  }
  for (int32_t i = 10; i < 15; i++) {
    data.emplace_back(folly::stringPrintf("key_b_%d", i), folly::stringPrintf("val_%d", i));
  }
  for (int32_t i = 20; i < 40; i++) {
    data.emplace_back(folly::stringPrintf("key_c_%d", i), folly::stringPrintf("val_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  }

  auto checkPrefix = [&](const std::string& prefix, int32_t expectedFrom, int32_t expectedTotal) {
    VLOG(1) << "prefix " << prefix << ", expectedFrom " << expectedFrom << ", expectedTotal "
            << expectedTotal;

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
  checkPrefix("key_a", 0, 10);
  checkPrefix("key_b", 10, 5);
  checkPrefix("key_c", 20, 20);
}

TEST_P(RocksEngineTest, RemoveTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_RemoveTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->put("key", "val"));
  std::string val;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &val));
  EXPECT_EQ("val", val);
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->remove("key"));
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, engine->get("key", &val));
}

TEST_P(RocksEngineTest, RemoveRangeTest) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
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
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
              engine->removeRange(std::string(reinterpret_cast<const char*>(&s), sizeof(int32_t)),
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

TEST_P(RocksEngineTest, OptionTest) {
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

TEST_P(RocksEngineTest, CompactTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_CompactTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  std::vector<KV> data;
  for (int32_t i = 2; i < 8; i++) {
    data.emplace_back(folly::stringPrintf("key_%d", i), folly::stringPrintf("value_%d", i));
  }
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->compact());
}

TEST_P(RocksEngineTest, IngestTest) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
  rocksdb::Options options;
  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
  fs::TempDir rootPath("/tmp/rocksdb_engine_IngestTest.XXXXXX");
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
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->ingest(files));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  }

  std::string result;
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key", &result));
  EXPECT_EQ("value", result);
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->get("key_empty", &result));
  EXPECT_EQ("", result);
  EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, engine->get("key_not_exist", &result));

  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->ingest({}));
}

TEST_P(RocksEngineTest, BackupRestoreTable) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
  rocksdb::Options options;
  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
  fs::TempDir rootPath("/tmp/rocksdb_engine_backuptable.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());

  std::vector<KV> data;
  for (int32_t i = 0; i < 10; i++) {
    data.emplace_back(folly::stringPrintf("part_%d", i), folly::stringPrintf("val_%d", i));
    data.emplace_back(folly::stringPrintf("tags_%d", i), folly::stringPrintf("val_%d", i));
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
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  }

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

TEST_P(RocksEngineTest, VertexWholeKeyBloomFilterTest) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
  FLAGS_enable_rocksdb_statistics = true;
  fs::TempDir rootPath("/tmp/rocksdb_engine_VertexBloomFilterTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  PartitionID partId = 1;
  VertexID vId = "vertex";
  VertexID nonexistent = "notExist";

  auto writeVertex = [&](TagID tagId) {
    std::vector<KV> data;
    data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, vId, tagId),
                      folly::stringPrintf("val_%d", tagId));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
  };

  auto readVertex = [&](TagID tagId) {
    auto key = NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, vId, tagId);
    std::string val;
    auto ret = engine->get(key, &val);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      EXPECT_EQ(folly::stringPrintf("val_%d", tagId), val);
    } else {
      EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, ret);
    }
  };

  auto scanVertex = [&](VertexID id) {
    auto prefix = NebulaKeyUtils::tagPrefix(kDefaultVIdLen, partId, id);
    std::unique_ptr<KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    EXPECT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);
  };

  auto statistics = kvstore::getDBStatistics();
  statistics->getAndResetTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL);
  statistics->getAndResetTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL);

  // write initial vertex
  writeVertex(0);

  // read data while in memtable
  if (FLAGS_enable_rocksdb_whole_key_filtering) {
    readVertex(0);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    readVertex(1);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
  }
  if (FLAGS_enable_rocksdb_prefix_filtering) {
    scanVertex(vId);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
    scanVertex(nonexistent);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
  }

  // flush to sst, read again
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  if (FLAGS_enable_rocksdb_whole_key_filtering) {
    readVertex(0);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    // read not exists data, whole key bloom filter will be useful
    readVertex(1);
    EXPECT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
  }
  if (FLAGS_enable_rocksdb_prefix_filtering) {
    scanVertex(vId);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
    // read not exists data, prefix key bloom filter will be useful
    scanVertex(nonexistent);
    EXPECT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
  }

  FLAGS_enable_rocksdb_statistics = false;
}

TEST_P(RocksEngineTest, EdgeWholeKeyBloomFilterTest) {
  if (FLAGS_rocksdb_table_format == "PlainTable") {
    return;
  }
  FLAGS_enable_rocksdb_statistics = true;
  fs::TempDir rootPath("/tmp/rocksdb_engine_EdgeBloomFilterTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());
  PartitionID partId = 1;
  VertexID vId = "vertex";
  VertexID nonexistent = "notExist";

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

  auto scanEdge = [&](VertexID id) {
    auto prefix = NebulaKeyUtils::edgePrefix(kDefaultVIdLen, partId, id);
    std::unique_ptr<KVIterator> iter;
    auto ret = engine->prefix(prefix, &iter);
    EXPECT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);
  };

  auto statistics = kvstore::getDBStatistics();
  statistics->getAndResetTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL);
  statistics->getAndResetTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL);

  // write initial vertex
  writeEdge(0);

  // read data while in memtable
  if (FLAGS_enable_rocksdb_whole_key_filtering) {
    readEdge(0);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    readEdge(1);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
  }
  if (FLAGS_enable_rocksdb_prefix_filtering) {
    scanEdge(vId);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
    scanEdge(nonexistent);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
  }

  // flush to sst, read again
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  if (FLAGS_enable_rocksdb_whole_key_filtering) {
    readEdge(0);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
    // read not exists data, whole key bloom filter will be useful
    readEdge(1);
    EXPECT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_USEFUL), 0);
  }
  if (FLAGS_enable_rocksdb_prefix_filtering) {
    scanEdge(vId);
    EXPECT_EQ(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
    // read not exists data, prefix key bloom filter will be useful
    scanEdge(nonexistent);
    EXPECT_GT(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL), 0);
  }

  FLAGS_enable_rocksdb_statistics = false;
}

TEST_P(RocksEngineTest, PrefixBloomTest) {
  fs::TempDir rootPath("/tmp/rocksdb_engine_PrefixExtractorTest.XXXXXX");
  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, rootPath.path());

  std::vector<KV> data;
  for (auto tagId = 0; tagId < 10; tagId++) {
    data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "1", tagId),
                      folly::stringPrintf("val_%d", tagId));
    data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "2", tagId),
                      folly::stringPrintf("val_%d", tagId));
    data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 2, "3", tagId),
                      folly::stringPrintf("val_%d", tagId));
    data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 2, "4", tagId),
                      folly::stringPrintf("val_%d", tagId));
  }
  data.emplace_back(NebulaKeyUtils::systemCommitKey(1), "123");
  data.emplace_back(NebulaKeyUtils::systemCommitKey(2), "123");
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->multiPut(std::move(data)));
  if (flush_) {
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, engine->flush());
  }

  {
    // vertexPrefix(partId) will not be included
    auto checkVertexPrefix = [&](PartitionID partId, const VertexID& vId) {
      std::string prefix = NebulaKeyUtils::tagPrefix(kDefaultVIdLen, partId, vId);
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
    // vertexPrefix(partId) will be included
    auto checkPartPrefix = [&](PartitionID partId) {
      std::string prefix = NebulaKeyUtils::tagPrefix(partId);
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
    // vertexPrefix(partId) will be included
    auto checkRangeWithPartPrefix = [&](PartitionID partId) {
      std::string prefix = NebulaKeyUtils::tagPrefix(partId);
      std::unique_ptr<KVIterator> iter;
      auto code = engine->rangeWithPrefix(prefix, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
      int32_t num = 0;
      while (iter->valid()) {
        num++;
        iter->next();
      }
      EXPECT_EQ(num, 20);
    };
    checkRangeWithPartPrefix(1);
    checkRangeWithPartPrefix(2);
  }
  {
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

INSTANTIATE_TEST_SUITE_P(EnablePrefixExtractor_EnableWholeKeyFilter_TableFormat_FlushOrNot,
                         RocksEngineTest,
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

TEST(PlainTableTest, BackupRestoreWithoutData) {
  fs::TempDir dataPath("/tmp/rocks_engine_test_data_path.XXXXXX");
  fs::TempDir rocksdbWalPath("/tmp/rocks_engine_test_rocksdb_wal_path.XXXXXX");
  fs::TempDir backupPath("/tmp/rocks_engine_test_backup_path.XXXXXX");
  FLAGS_rocksdb_table_format = "PlainTable";
  FLAGS_rocksdb_wal_dir = rocksdbWalPath.path();
  FLAGS_rocksdb_backup_dir = backupPath.path();
  FLAGS_enable_rocksdb_prefix_filtering = true;

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
  FLAGS_enable_rocksdb_prefix_filtering = false;
}

TEST(PlainTableTest, BackupRestoreWithData) {
  fs::TempDir dataPath("/tmp/rocks_engine_test_data_path.XXXXXX");
  fs::TempDir rocksdbWalPath("/tmp/rocks_engine_test_rocksdb_wal_path.XXXXXX");
  fs::TempDir backupPath("/tmp/rocks_engine_test_backup_path.XXXXXX");
  FLAGS_rocksdb_table_format = "PlainTable";
  FLAGS_rocksdb_wal_dir = rocksdbWalPath.path();
  FLAGS_rocksdb_backup_dir = backupPath.path();
  FLAGS_enable_rocksdb_prefix_filtering = true;

  auto engine = std::make_unique<RocksEngine>(0, kDefaultVIdLen, dataPath.path());
  PartitionID partId = 1;

  auto checkData = [&] {
    {
      std::string prefix = NebulaKeyUtils::tagPrefix(kDefaultVIdLen, partId, "vertex");
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
      std::string prefix = NebulaKeyUtils::tagPrefix(partId);
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

    std::string value;
    auto code = engine->get(NebulaKeyUtils::systemCommitKey(partId), &value);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    EXPECT_EQ("123", value);
  };

  LOG(INFO) << "Write some data";
  std::vector<KV> data;
  for (auto tagId = 0; tagId < 10; tagId++) {
    data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, partId, "vertex", tagId),
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
  FLAGS_enable_rocksdb_prefix_filtering = false;
}

TEST(RebuildPrefixBloomFilter, RebuildPrefixBloomFilter) {
  GraphSpaceID spaceId = 1;
  // previously default config (prefix off whole on)
  FLAGS_rocksdb_table_format = "BlockBasedTable";
  FLAGS_enable_rocksdb_prefix_filtering = false;
  FLAGS_enable_rocksdb_whole_key_filtering = true;

  fs::TempDir dataPath("/tmp/rocksdb_engine_rebuild_prefix_bloom_filter.XXXXXX");
  LOG(INFO) << "start the engine with prefix bloom filter disabled";
  auto engine = std::make_unique<RocksEngine>(spaceId, kDefaultVIdLen, dataPath.path());

  auto checkData = [&] {
    auto checkVertexPrefix = [&](PartitionID partId, VertexID vId) {
      {
        std::string prefix = NebulaKeyUtils::tagPrefix(kDefaultVIdLen, partId, vId);
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
      for (TagID tagId = 0; tagId < 10; tagId++) {
        std::string prefix = NebulaKeyUtils::tagPrefix(kDefaultVIdLen, partId, vId, tagId);
        std::unique_ptr<KVIterator> iter;
        auto code = engine->prefix(prefix, &iter);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        int32_t num = 0;
        while (iter->valid()) {
          num++;
          iter->next();
        }
        EXPECT_EQ(num, 1);
      }
    };

    auto checkEdgePrefix = [&](PartitionID partId, VertexID vId) {
      {
        std::string prefix = NebulaKeyUtils::edgePrefix(kDefaultVIdLen, partId, vId);
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
      for (EdgeType edgeType = 0; edgeType < 10; edgeType++) {
        std::string prefix = NebulaKeyUtils::edgePrefix(kDefaultVIdLen, partId, vId, edgeType);
        std::unique_ptr<KVIterator> iter;
        auto code = engine->prefix(prefix, &iter);
        EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        int32_t num = 0;
        while (iter->valid()) {
          num++;
          iter->next();
        }
        EXPECT_EQ(num, 1);
      }
    };

    auto checkVertexPartPrefix = [&](PartitionID partId) {
      std::string prefix = NebulaKeyUtils::tagPrefix(partId);
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

    auto checkEdgePartPrefix = [&](PartitionID partId) {
      std::string prefix = NebulaKeyUtils::edgePrefix(partId);
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

    auto checkRangeWithPartPrefix = [&](PartitionID partId) {
      std::string prefix = NebulaKeyUtils::tagPrefix(partId);
      std::unique_ptr<KVIterator> iter;
      auto code = engine->rangeWithPrefix(prefix, prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
      int32_t num = 0;
      while (iter->valid()) {
        num++;
        iter->next();
      }
      EXPECT_EQ(num, 20);
    };

    auto checkSystemCommit = [&](PartitionID partId) {
      std::string value;
      auto code = engine->get(NebulaKeyUtils::systemCommitKey(partId), &value);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
      EXPECT_EQ("123", value);
    };

    auto checkSystemPart = [&]() { EXPECT_EQ(10, engine->allParts().size()); };

    checkVertexPrefix(1, "1");
    checkVertexPrefix(1, "2");
    checkVertexPrefix(2, "3");
    checkVertexPrefix(2, "4");
    checkEdgePrefix(1, "1");
    checkEdgePrefix(1, "2");
    checkEdgePrefix(2, "3");
    checkEdgePrefix(2, "4");
    checkVertexPartPrefix(1);
    checkVertexPartPrefix(2);
    checkEdgePartPrefix(1);
    checkEdgePartPrefix(2);
    checkRangeWithPartPrefix(1);
    checkRangeWithPartPrefix(2);
    checkSystemPart();
    checkSystemCommit(1);
    checkSystemCommit(2);
  };

  auto writeData = [&engine] {
    for (PartitionID partId = 1; partId <= 10; partId++) {
      engine->addPart(partId);
    }

    LOG(INFO) << "Write some data";
    std::vector<KV> data;
    for (TagID tagId = 0; tagId < 10; tagId++) {
      data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "1", tagId),
                        folly::stringPrintf("val_%d", tagId));
      data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 1, "2", tagId),
                        folly::stringPrintf("val_%d", tagId));
      data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 2, "3", tagId),
                        folly::stringPrintf("val_%d", tagId));
      data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 2, "4", tagId),
                        folly::stringPrintf("val_%d", tagId));
    }
    EdgeRanking rank = 0;
    for (EdgeType edgeType = 0; edgeType < 10; edgeType++) {
      data.emplace_back(NebulaKeyUtils::edgeKey(kDefaultVIdLen, 1, "1", edgeType, rank, "1"),
                        folly::stringPrintf("val_%d", edgeType));
      data.emplace_back(NebulaKeyUtils::edgeKey(kDefaultVIdLen, 1, "2", edgeType, rank, "2"),
                        folly::stringPrintf("val_%d", edgeType));
      data.emplace_back(NebulaKeyUtils::edgeKey(kDefaultVIdLen, 2, "3", edgeType, rank, "3"),
                        folly::stringPrintf("val_%d", edgeType));
      data.emplace_back(NebulaKeyUtils::edgeKey(kDefaultVIdLen, 2, "4", edgeType, rank, "4"),
                        folly::stringPrintf("val_%d", edgeType));
    }
    data.emplace_back(NebulaKeyUtils::systemCommitKey(1), "123");
    data.emplace_back(NebulaKeyUtils::systemCommitKey(2), "123");
    auto code = engine->multiPut(std::move(data));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  };

  auto writeNewData = [&engine] {
    std::vector<KV> data;
    data.emplace_back(NebulaKeyUtils::tagKey(kDefaultVIdLen, 3, "5", 0),
                      "vertex_data_after_enable_prefix_bloom_filter");
    data.emplace_back(NebulaKeyUtils::edgeKey(kDefaultVIdLen, 3, "5", 0, 0, "5"),
                      "edge_data_after_enable_prefix_bloom_filter");
    auto code = engine->multiPut(std::move(data));
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
  };

  auto checkNewData = [&engine] {
    std::string value;
    auto code = engine->get(NebulaKeyUtils::tagKey(kDefaultVIdLen, 3, "5", 0), &value);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    EXPECT_EQ("vertex_data_after_enable_prefix_bloom_filter", value);
    code = engine->get(NebulaKeyUtils::edgeKey(kDefaultVIdLen, 3, "5", 0, 0, "5"), &value);
    EXPECT_EQ("edge_data_after_enable_prefix_bloom_filter", value);

    auto checkPrefix = [&](const std::string& prefix) {
      std::unique_ptr<KVIterator> iter;
      code = engine->prefix(prefix, &iter);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
      int32_t num = 0;
      while (iter->valid()) {
        num++;
        iter->next();
      }
      EXPECT_EQ(num, 1);
    };

    checkPrefix(NebulaKeyUtils::tagPrefix(3));
    checkPrefix(NebulaKeyUtils::tagPrefix(kDefaultVIdLen, 3, "5"));
    checkPrefix(NebulaKeyUtils::tagPrefix(kDefaultVIdLen, 3, "5", 0));
    checkPrefix(NebulaKeyUtils::edgePrefix(3));
    checkPrefix(NebulaKeyUtils::edgePrefix(kDefaultVIdLen, 3, "5"));
    checkPrefix(NebulaKeyUtils::edgePrefix(kDefaultVIdLen, 3, "5", 0));
    checkPrefix(NebulaKeyUtils::edgePrefix(kDefaultVIdLen, 3, "5", 0, 0, "5"));
  };

  writeData();
  checkData();

  LOG(INFO) << "release the engine and restart with prefix bloom filter enabled";
  engine.reset();
  // new default config (prefix on whole off)
  FLAGS_enable_rocksdb_prefix_filtering = true;
  FLAGS_enable_rocksdb_whole_key_filtering = false;
  engine = std::make_unique<RocksEngine>(spaceId, kDefaultVIdLen, dataPath.path());
  checkData();

  writeNewData();
  checkNewData();

  LOG(INFO) << "compact to rebuild prefix bloom filter";
  engine->compact();
  checkData();
  checkNewData();

  LOG(INFO) << "release the engine and restart with prefix bloom filter disabled again";
  engine.reset();
  FLAGS_enable_rocksdb_prefix_filtering = false;
  FLAGS_enable_rocksdb_whole_key_filtering = true;
  engine = std::make_unique<RocksEngine>(spaceId, kDefaultVIdLen, dataPath.path());
  checkData();
  checkNewData();

  LOG(INFO) << "compact to rebuild whole key bloom filter";
  engine->compact();
  checkData();
  checkNewData();
}

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
