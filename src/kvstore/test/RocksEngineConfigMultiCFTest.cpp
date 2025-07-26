/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <rocksdb/cache.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/utilities/options_util.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "kvstore/RocksEngine.h"
#include "kvstore/RocksEngineConfig.h"

#define KV_DATA_PATH_FORMAT(path, spaceId) folly::stringPrintf("%s/nebula/%d/data", path, spaceId)

namespace nebula {
namespace kvstore {

TEST(RocksEngineConfigTest, MultiCFSimpleOptionTest) {
  fs::TempDir rootPath("/tmp/MultiCFSimpleOptionTest.XXXXXX");

  FLAGS_rocksdb_db_options = R"({
                                   "stats_dump_period_sec":"200",
                                   "enable_write_thread_adaptive_yield":"false",
                                   "write_thread_max_yield_usec":"600"
                               })";
  FLAGS_rocksdb_column_family_options = R"({
                                              "max_write_buffer_number":"4",
                                              "min_write_buffer_number_to_merge":"2",
                                              "max_write_buffer_number_to_maintain":"1"
                                          })";
  FLAGS_rocksdb_block_based_table_options = R"({"block_restart_interval":"2"})";

  // Create the RocksEngine instance, which will internally open a multi-CF DB
  int defaultVidLen = 8;
  {
    auto engine = std::make_unique<RocksEngine>(0, defaultVidLen, rootPath.path());
    // engine goes out of scope and gets destroyed, closing the DB and persisting options.
  }

  ASSERT_NE(nebula::fs::FileType::NOTEXIST, nebula::fs::FileUtils::fileType(rootPath.path()));
  ASSERT_EQ(nebula::fs::FileType::DIRECTORY, nebula::fs::FileUtils::fileType(rootPath.path()));

  // Load the persisted options from the OPTIONS file
  rocksdb::DBOptions loadedDbOpt;
  std::vector<rocksdb::ColumnFamilyDescriptor> loadedCfDescs;
  rocksdb::Status s = LoadLatestOptions(KV_DATA_PATH_FORMAT(rootPath.path(), 0),
                                        rocksdb::Env::Default(),
                                        &loadedDbOpt,
                                        &loadedCfDescs);
  ASSERT_TRUE(s.ok()) << "Unexpected error happens when loading the option file from \""
                      << rootPath.path() << "\": " << s.ToString();

  // 1. Verify DBOptions
  EXPECT_EQ(200, loadedDbOpt.stats_dump_period_sec);
  EXPECT_FALSE(loadedDbOpt.enable_write_thread_adaptive_yield);
  EXPECT_EQ(600, loadedDbOpt.write_thread_max_yield_usec);

  // 2. Verify ColumnFamilyOptions for all created CFs
  // RocksEngine creates "default", "data", "meta"
  ASSERT_EQ(3, loadedCfDescs.size());

  for (const auto& desc : loadedCfDescs) {
    LOG(INFO) << "Checking options for Column Family: " << desc.name;
    const auto& cfOpts = desc.options;
    EXPECT_EQ(4, cfOpts.max_write_buffer_number);
    EXPECT_EQ(2, cfOpts.min_write_buffer_number_to_merge);
    EXPECT_EQ(1, cfOpts.max_write_buffer_number_to_maintain);

    // 3. Verify BlockBasedTableOptions for each CF
    ASSERT_NE(nullptr, cfOpts.table_factory);
    auto* bbtOpts = cfOpts.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
    ASSERT_NE(nullptr, bbtOpts);
    EXPECT_EQ(2, bbtOpts->block_restart_interval);
  }

  // Clean up
  FLAGS_rocksdb_db_options = "{}";
  FLAGS_rocksdb_column_family_options = "{}";
  FLAGS_rocksdb_block_based_table_options = "{}";
}

TEST(RocksEngineConfigTest, MultiCFCreateOptionsTest) {
  rocksdb::DBOptions dbOptions;
  rocksdb::ColumnFamilyOptions cfOptions;
  FLAGS_rocksdb_db_options = R"({"stats_dump_period_sec":"aaaaaa"})";

  rocksdb::Status s = initRocksdbOptions(dbOptions, cfOptions, 1);

  ASSERT_EQ(rocksdb::Status::kInvalidArgument, s.code());
  EXPECT_EQ("Invalid argument: Error parsing stats_dump_period_sec:stoull", s.ToString());

  // Clean up
  FLAGS_rocksdb_db_options = "{}";
}

TEST(RocksEngineConfigTest, MultiCFStatisticsConfigTest) {
  {
    FLAGS_enable_rocksdb_statistics = false;
    rocksdb::DBOptions dbOptions;
    rocksdb::ColumnFamilyOptions cfOptions;
    auto status = initRocksdbOptions(dbOptions, cfOptions, 1);
    ASSERT_TRUE(status.ok()) << status.ToString();

    ASSERT_EQ(nullptr, dbOptions.statistics);
    ASSERT_EQ(nullptr, getDBStatistics());
  }

  {
    FLAGS_enable_rocksdb_statistics = true;
    FLAGS_rocksdb_stats_level = "kExceptTimers";
    rocksdb::DBOptions dbOptions;
    rocksdb::ColumnFamilyOptions cfOptions;
    auto status = initRocksdbOptions(dbOptions, cfOptions, 1);
    ASSERT_TRUE(status.ok()) << status.ToString();

    ASSERT_NE(nullptr, dbOptions.statistics);
    ASSERT_EQ(rocksdb::StatsLevel::kExceptTimers, dbOptions.statistics->get_stats_level());

    std::shared_ptr<rocksdb::Statistics> stats = getDBStatistics();
    ASSERT_NE(nullptr, stats);
    ASSERT_EQ(rocksdb::StatsLevel::kExceptTimers, stats->get_stats_level());
  }
}

TEST(RocksDBEngineTest, MultiCFCompressionConfigTest) {
  FLAGS_rocksdb_compression = "lz4";
  FLAGS_rocksdb_compression_per_level = "";
  FLAGS_rocksdb_bottommost_compression = "zstd";

  rocksdb::DBOptions dbOptions;
  rocksdb::ColumnFamilyOptions baseCfOptions;
  auto status = initRocksdbOptions(dbOptions, baseCfOptions, 1);
  ASSERT_TRUE(status.ok()) << status.ToString();

  ASSERT_EQ(rocksdb::kLZ4Compression, baseCfOptions.compression);
  ASSERT_EQ(rocksdb::kZSTD, baseCfOptions.bottommost_compression);

  std::vector<std::string> cfNames = {"default", "vector", "idvidmap"};
  std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;
  rocksdb::ColumnFamilyOptions vectorCfOptions = baseCfOptions;
  rocksdb::ColumnFamilyOptions mapCfOptions = baseCfOptions;

  cfDescriptors.emplace_back("default", baseCfOptions);
  cfDescriptors.emplace_back("vector", vectorCfOptions);
  cfDescriptors.emplace_back("idvidmap", mapCfOptions);

  rocksdb::DB* db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  SCOPE_EXIT {
    if (db != nullptr) {
      for (auto* handle : handles) {
        db->DestroyColumnFamilyHandle(handle);
      }
      delete db;
    }
  };

  dbOptions.create_if_missing = true;
  fs::TempDir rootPath("/tmp/RocksDBMultiCFCompressionConfigTest.XXXXXX");
  rocksdb::Options singleCFOptions;
  initRocksdbOptions(singleCFOptions, 1);
  status = rocksdb::DB::Open(singleCFOptions, rootPath.path(), &db);
  ASSERT_TRUE(status.ok()) << status.ToString();
  for (const auto& name : cfNames) {
    if (name == NebulaKeyUtils::NebulaKeyUtils::kDefaultColumnFamilyName) {
      continue;
    }
    rocksdb::ColumnFamilyHandle* cfh;
    status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &cfh);
    if (status.ok()) {
      LOG(INFO) << "Column family '" << name << "' created successfully.";
      db->DestroyColumnFamilyHandle(cfh);
    } else {
      LOG(ERROR) << "Failed to create column family '" << name << "': " << status.ToString();
    }
  }

  delete db;
  db = nullptr;

  status = rocksdb::DB::Open(dbOptions, rootPath.path(), cfDescriptors, &handles, &db);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_NE(nullptr, db);

  ASSERT_EQ(cfNames.size(), handles.size());
  for (const auto& handle : handles) {
    ASSERT_NE(nullptr, handle);
  }
}

TEST(RocksEngineConfigTest, MultiCFKeyValueSeparationTest) {
  FLAGS_rocksdb_enable_kv_separation = true;
  FLAGS_rocksdb_kv_separation_threshold = 10;

  rocksdb::DBOptions dbOptions;
  rocksdb::ColumnFamilyOptions baseCfOptions;
  auto status = initRocksdbOptions(dbOptions, baseCfOptions, 1);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<std::string> cfNames = {"default", "data", "meta"};
  std::vector<rocksdb::ColumnFamilyDescriptor> cfDescriptors;

  rocksdb::ColumnFamilyOptions defaultCfOptions = baseCfOptions;
  rocksdb::ColumnFamilyOptions dataCfOptions = baseCfOptions;
  rocksdb::ColumnFamilyOptions metaCfOptions = baseCfOptions;

  ASSERT_TRUE(dataCfOptions.enable_blob_files);  // 确认配置已开启
  ASSERT_EQ(10, dataCfOptions.min_blob_size);

  cfDescriptors.emplace_back("default", defaultCfOptions);
  cfDescriptors.emplace_back("data", dataCfOptions);
  cfDescriptors.emplace_back("meta", metaCfOptions);

  rocksdb::DB* db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  SCOPE_EXIT {
    if (db != nullptr) {
      for (auto* handle : handles) {
        db->DestroyColumnFamilyHandle(handle);
      }
      delete db;
    }
  };

  dbOptions.create_if_missing = true;
  fs::TempDir rootPath("/tmp/RocksDBMultiCFKVSeparationTest.XXXXXX");

  rocksdb::Options singleCFOptions;
  initRocksdbOptions(singleCFOptions, 1);
  status = rocksdb::DB::Open(singleCFOptions, rootPath.path(), &db);
  ASSERT_TRUE(status.ok()) << status.ToString();
  for (const auto& name : cfNames) {
    if (name == NebulaKeyUtils::kDefaultColumnFamilyName) {
      continue;
    }
    rocksdb::ColumnFamilyHandle* cfh;
    status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &cfh);
    if (status.ok()) {
      LOG(INFO) << "Column family '" << name << "' created successfully.";
      db->DestroyColumnFamilyHandle(cfh);
    } else {
      LOG(ERROR) << "Failed to create column family '" << name << "': " << status.ToString();
    }
  }

  delete db;
  db = nullptr;

  status = rocksdb::DB::Open(dbOptions, rootPath.path(), cfDescriptors, &handles, &db);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_NE(nullptr, db);
  ASSERT_EQ(cfNames.size(), handles.size());

  // 6. 验证读写
  std::string key = "test_key";
  std::string large_value = "This is a test value with value size greater than 10";
  rocksdb::WriteOptions wOpts;
  rocksdb::ReadOptions rOpts;

  status = db->Put(wOpts, handles[1], key, large_value);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::string read_value_data;
  status = db->Get(rOpts, handles[1], key, &read_value_data);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(large_value, read_value_data);

  status = db->Put(wOpts, handles[2], key, large_value);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::string read_value_meta;
  status = db->Get(rOpts, handles[2], key, &read_value_meta);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(large_value, read_value_meta);

  std::string read_value_default;
  status = db->Get(rOpts, handles[0], key, &read_value_default);
  ASSERT_TRUE(status.IsNotFound());
}

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
