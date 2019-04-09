/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include <kvstore/RocksEngine.h>
#include "fs/TempDir.h"
#include "kvstore/RocksEngineConfig.h"

namespace nebula {
namespace kvstore {

TEST(RocksEngineConfigTest, simpleOptionTest) {
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");

    FLAGS_rocksdb_db_options = "stats_dump_period_sec=200;"
                               "enable_write_thread_adaptive_yield=false;"
                               "write_thread_max_yield_usec=600";
    FLAGS_rocksdb_column_family_options = "max_write_buffer_number=4;"
                                          "min_write_buffer_number_to_merge=2;"
                                          "max_write_buffer_number_to_maintain=1";
    FLAGS_rocksdb_block_based_table_options = "block_restart_interval=2";

    // Create the RocksEngine instance
    auto engine = std::make_unique<RocksEngine>(0, KV_DATA_PATH_FORMAT(rootPath.path(), 0));
    engine.reset();

    ASSERT_NE(nebula::fs::FileType::NOTEXIST,
              nebula::fs::FileUtils::fileType(rootPath.path()));
    ASSERT_EQ(nebula::fs::FileType::DIRECTORY,
              nebula::fs::FileUtils::fileType(rootPath.path()));

    rocksdb::DBOptions loadedDbOpt;
    std::vector<rocksdb::ColumnFamilyDescriptor> loadedCfDescs;
    rocksdb::Status s = LoadLatestOptions(
            KV_DATA_PATH_FORMAT(rootPath.path(), 0),
            rocksdb::Env::Default(),
            &loadedDbOpt,
            &loadedCfDescs);
    ASSERT_TRUE(s.ok());

    EXPECT_EQ(200, loadedDbOpt.stats_dump_period_sec);
    EXPECT_EQ(false, loadedDbOpt.enable_write_thread_adaptive_yield);
    EXPECT_EQ(600, loadedDbOpt.write_thread_max_yield_usec);
    EXPECT_EQ(4, loadedCfDescs[0].options.max_write_buffer_number);
    EXPECT_EQ(2, loadedCfDescs[0].options.min_write_buffer_number_to_merge);
    EXPECT_EQ(1, loadedCfDescs[0].options.max_write_buffer_number_to_maintain);

    auto loadedBbtOpt = reinterpret_cast<rocksdb::BlockBasedTableOptions*>(
        loadedCfDescs[0].options.table_factory->GetOptions());
    EXPECT_EQ(2, loadedBbtOpt->block_restart_interval);

    // Clean up
    FLAGS_rocksdb_db_options = "";
    FLAGS_rocksdb_column_family_options = "";
    FLAGS_rocksdb_block_based_table_options = "";
}


TEST(RocksEngineConfigTest, createOptionsTest) {
    rocksdb::Options options;
    FLAGS_rocksdb_db_options = "stats_dump_period_sec=aaaaaa";

    rocksdb::Status s = initRocksdbOptions(options);
    ASSERT_EQ(rocksdb::Status::kInvalidArgument , s.code());
    EXPECT_EQ("Invalid argument: Unable to parse DBOptions:: stats_dump_period_sec",
              s.ToString());

    // Clean up
    FLAGS_rocksdb_db_options = "";
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
