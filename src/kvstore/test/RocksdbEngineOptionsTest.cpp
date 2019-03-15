/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <kvstore/RocksdbEngine.h>
#include "fs/TempDir.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/RocksdbConfigFlags.h"
#include "kvstore/RocksdbConfigOptions.h"

namespace nebula {
namespace kvstore {

TEST(RocksdbEngineOptionsTest, simpleOptionTest) {
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        rocksdb::BlockBasedTableOptions *loadedBbtOpt;
    FLAGS_rocksdb_db_options = "stats_dump_period_sec=200;"
                               "enable_write_thread_adaptive_yield=false;"
                               "write_thread_max_yield_usec=600";
    FLAGS_rocksdb_column_family_options = "max_write_buffer_number=4;"
                                          "min_write_buffer_number_to_merge=2;"
                                          "max_write_buffer_number_to_maintain=1";
    FLAGS_rocksdb_block_based_table_options = "block_restart_interval=2";
    std::unique_ptr<RocksdbEngine> engine = nullptr;
            std::make_unique<RocksdbEngine>(0, KV_DATA_PATH_FORMAT(rootPath.path(), 0));
    engine.reset(nullptr);

    ASSERT_FALSE(nebula::fs::FileUtils::fileType(rootPath.path()) ==
    nebula::fs::FileType::NOTEXIST);
    ASSERT_EQ(nebula::fs::FileUtils::fileType(rootPath.path()), nebula::fs::FileType::DIRECTORY);

    rocksdb::DBOptions loadedDbOpt;
    std::vector<rocksdb::ColumnFamilyDescriptor> loadedCfDescs;
    rocksdb::Status s = LoadLatestOptions(
            KV_DATA_PATH_FORMAT(rootPath.path(), 0),
            rocksdb::Env::Default(),
            &loadedDbOpt,
            &loadedCfDescs);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(loadedDbOpt.stats_dump_period_sec, 200);
    ASSERT_EQ(loadedDbOpt.enable_write_thread_adaptive_yield, false);
    ASSERT_EQ(loadedDbOpt.write_thread_max_yield_usec, 600);
    ASSERT_EQ(loadedCfDescs[0].options.max_write_buffer_number, 4);
    ASSERT_EQ(loadedCfDescs[0].options.min_write_buffer_number_to_merge, 2);
    ASSERT_EQ(loadedCfDescs[0].options.max_write_buffer_number_to_maintain, 1);
    loadedBbtOpt = reinterpret_cast<rocksdb::BlockBasedTableOptions*>(
                loadedCfDescs[0].options.table_factory->GetOptions());
    ASSERT_EQ(loadedBbtOpt->block_restart_interval, 2);
    FLAGS_rocksdb_db_options = "";
    FLAGS_rocksdb_column_family_options = "";
    FLAGS_rocksdb_block_based_table_options = "";
}

TEST(RocksdbEngineOptionsTest, createOptionsTest) {
    rocksdb::Options options;
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    FLAGS_rocksdb_db_options = "stats_dump_period_sec=aaaaaa";
    rocksdb::Status s = std::make_shared<RocksdbConfigOptions>()
            ->initRocksdbOptions(rootPath.path());
    FLAGS_rocksdb_db_options = "";
    ASSERT_EQ(rocksdb::Status::kInvalidArgument , s.code());
    ASSERT_EQ("Invalid argument: Unable to parse DBOptions:: stats_dump_period_sec",
            s.ToString());
}
}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
