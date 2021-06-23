/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/cache.h>
#include <rocksdb/convenience.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/slice_transform.h>
#include "kvstore/RocksEngine.h"
#include "kvstore/RocksEngineConfig.h"

#define KV_DATA_PATH_FORMAT(path, spaceId) \
     folly::stringPrintf("%s/nebula/%d/data", path, spaceId)

namespace nebula {
namespace kvstore {

TEST(RocksEngineConfigTest, SimpleOptionTest) {
    fs::TempDir rootPath("/tmp/SimpleOptionTest.XXXXXX");

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

    // Create the RocksEngine instance
    int defaultVidLen = 8;
    auto engine = std::make_unique<RocksEngine>(0, defaultVidLen, rootPath.path());
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
    ASSERT_TRUE(s.ok())
        << "Unexpected error happens when loading the option file from \""
        << KV_DATA_PATH_FORMAT(rootPath.path(), 0)
        << "\": " << s.ToString();

    EXPECT_EQ(200, loadedDbOpt.stats_dump_period_sec);
    EXPECT_EQ(false, loadedDbOpt.enable_write_thread_adaptive_yield);
    EXPECT_EQ(600, loadedDbOpt.write_thread_max_yield_usec);
    EXPECT_EQ(4, loadedCfDescs[0].options.max_write_buffer_number);
    EXPECT_EQ(2, loadedCfDescs[0].options.min_write_buffer_number_to_merge);
    EXPECT_EQ(1, loadedCfDescs[0].options.max_write_buffer_number_to_maintain);

    auto loadedBbtOpt = reinterpret_cast<rocksdb::BlockBasedTableOptions*>(
        loadedCfDescs[0].options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>());
    EXPECT_EQ(2, loadedBbtOpt->block_restart_interval);

    // Clean up
    FLAGS_rocksdb_db_options = "{}";
    FLAGS_rocksdb_column_family_options = "{}";
    FLAGS_rocksdb_block_based_table_options = "{}";
}


TEST(RocksEngineConfigTest, createOptionsTest) {
    rocksdb::Options options;
    FLAGS_rocksdb_db_options = R"({"stats_dump_period_sec":"aaaaaa"})";

    rocksdb::Status s = initRocksdbOptions(options, 1);
    ASSERT_EQ(rocksdb::Status::kInvalidArgument , s.code());
    EXPECT_EQ("Invalid argument: Error parsing stats_dump_period_sec:stoull",
              s.ToString());

    // Clean up
    FLAGS_rocksdb_db_options = "{}";
}

TEST(RocksEngineConfigTest, StatisticsConfigTest) {
    {
        FLAGS_enable_rocksdb_statistics = false;
        rocksdb::Options options;
        auto status = initRocksdbOptions(options, 1);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(nullptr, getDBStatistics());
    }

    {
        FLAGS_enable_rocksdb_statistics = true;
        FLAGS_rocksdb_stats_level = "kExceptTimers";
        rocksdb::Options options;
        auto status = initRocksdbOptions(options, 1);
        ASSERT_TRUE(status.ok()) << status.ToString();
        std::shared_ptr<rocksdb::Statistics> stats = getDBStatistics();
        ASSERT_EQ(rocksdb::StatsLevel::kExceptTimers, stats->get_stats_level());
    }
}

TEST(RocksEngineConfigTest, CompressionConfigTest) {
    {
        FLAGS_rocksdb_compression = "lz4";
        FLAGS_rocksdb_compression_per_level = "no:no::mp3::zstd";
        rocksdb::Options options;
        auto status = initRocksdbOptions(options, 1);
        ASSERT_EQ(rocksdb::Status::kInvalidArgument, status.code());
    }

    {
        FLAGS_rocksdb_compression = "lz4";
        FLAGS_rocksdb_compression_per_level = "no:no::snappy::zstd";
        rocksdb::Options options;
        auto status = initRocksdbOptions(options, 1);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(rocksdb::kLZ4Compression, options.compression);
        ASSERT_EQ(rocksdb::kNoCompression, options.compression_per_level[0]);
        ASSERT_EQ(rocksdb::kNoCompression, options.compression_per_level[1]);
        ASSERT_EQ(rocksdb::kLZ4Compression, options.compression_per_level[2]);
        ASSERT_EQ(rocksdb::kSnappyCompression, options.compression_per_level[3]);
        ASSERT_EQ(rocksdb::kLZ4Compression, options.compression_per_level[4]);
        ASSERT_EQ(rocksdb::kZSTD, options.compression_per_level[5]);
        ASSERT_EQ(rocksdb::kLZ4Compression, options.compression_per_level[6]);

        rocksdb::DB* db = nullptr;
        SCOPE_EXIT { delete db; };
        options.create_if_missing = true;
        fs::TempDir rootPath("/tmp/RocksDBCompressionConfigTest.XXXXXX");
        status = rocksdb::DB::Open(options, rootPath.path(), &db);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    {
        FLAGS_rocksdb_compression = "snappy";
        FLAGS_rocksdb_compression_per_level = "no:snappy:lz4:lz4hc:zstd:zlib:bzip2";
        rocksdb::Options options;
        auto status = initRocksdbOptions(options, 1);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(rocksdb::kSnappyCompression, options.compression);
        ASSERT_EQ(rocksdb::kNoCompression, options.compression_per_level[0]);
        ASSERT_EQ(rocksdb::kSnappyCompression, options.compression_per_level[1]);
        ASSERT_EQ(rocksdb::kLZ4Compression, options.compression_per_level[2]);
        ASSERT_EQ(rocksdb::kLZ4HCCompression, options.compression_per_level[3]);
        ASSERT_EQ(rocksdb::kZSTD, options.compression_per_level[4]);
        ASSERT_EQ(rocksdb::kZlibCompression, options.compression_per_level[5]);
        ASSERT_EQ(rocksdb::kBZip2Compression, options.compression_per_level[6]);

        rocksdb::DB* db = nullptr;
        SCOPE_EXIT { delete db; };
        options.create_if_missing = true;
        fs::TempDir rootPath("/tmp/RocksDBCompressionConfigTest.XXXXXX");
        status = rocksdb::DB::Open(options, rootPath.path(), &db);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
