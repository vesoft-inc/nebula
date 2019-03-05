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


TEST(RocksdbEngineOptionsTest, versionTest) {
    rocksdb::Options options;
    FLAGS_rocksdb_options_version = "111.111.1";
    rocksdb::Status s = std::make_shared<RocksdbConfigOptions>()
            ->createRocksdbEngineOptions(false, false);
    FLAGS_rocksdb_options_version = "5.15.10";
    ASSERT_EQ(rocksdb::Status::kNotSupported , s.code());
}

TEST(RocksdbEngineOptionsTest, simpleOptionTest) {
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    FLAGS_stats_dump_period_sec = "200";
    std::unique_ptr<RocksdbEngine> engine =
            std::make_unique<RocksdbEngine>(0, KV_DATA_PATH_FORMAT(rootPath.path(), 0),
                    KV_WAL_PATH_FORMAT(rootPath.path(), 0));
    engine.reset(nullptr);

    ASSERT_FALSE(nebula::fs::FileUtils::fileType(rootPath.path()) ==
    nebula::fs::FileType::NOTEXIST);
    ASSERT_EQ(nebula::fs::FileUtils::fileType(rootPath.path()), nebula::fs::FileType::DIRECTORY);

    rocksdb::DBOptions loaded_db_opt;
    std::vector<rocksdb::ColumnFamilyDescriptor> loaded_cf_descs;
    rocksdb::Status s = LoadLatestOptions(
            KV_DATA_PATH_FORMAT(rootPath.path(), 0),
            rocksdb::Env::Default(),
            &loaded_db_opt,
            &loaded_cf_descs);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(loaded_db_opt.stats_dump_period_sec,
            static_cast<int>(strtol(FLAGS_stats_dump_period_sec.c_str(), NULL, 10)));
}

TEST(RocksdbEngineOptionsTest, createOptionsTest) {
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    FLAGS_stats_dump_period_sec = "aaaaa";
    rocksdb::Status s = std::make_shared<RocksdbConfigOptions>()
            ->createRocksdbEngineOptions(false, false);
    FLAGS_stats_dump_period_sec = "600";
    ASSERT_EQ(rocksdb::Status::kInvalidArgument , s.code());
    ASSERT_EQ("Invalid argument: Unable to parse DBOptions:: stats_dump_period_sec",
            s.ToString());
}

TEST(RocksdbEngineOptionsTest, getOptionValueTest) {
    rocksdb::Options options;
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    FLAGS_stats_dump_period_sec = "200";
    rocksdb::Status s =
            std::make_shared<RocksdbConfigOptions>()
                    ->createRocksdbEngineOptions(false, false);
    std::string value;
    ASSERT_FALSE(RocksdbConfigOptions::getRocksdbEngineOptionValue(
            RocksdbConfigOptions::ROCKSDB_OPTION_TYPE::DBOPT,
            FLAGS_stats_dump_period_sec.c_str(),
            value));
    ASSERT_TRUE(RocksdbConfigOptions::getRocksdbEngineOptionValue(
            RocksdbConfigOptions::ROCKSDB_OPTION_TYPE::DBOPT,
            "stats_dump_period_sec",
            value));
    ASSERT_EQ(FLAGS_stats_dump_period_sec, value);
    FLAGS_stats_dump_period_sec = "600";
}

TEST(RocksdbEngineOptionsTest, pathsTest) {
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    KVPaths kv_paths;
    ASSERT_FALSE(RocksdbConfigOptions::getKVPaths(
            folly::stringPrintf("%s/disk1", rootPath.path()), "", kv_paths));
    ASSERT_FALSE(RocksdbConfigOptions::getKVPaths("", "", kv_paths));
    ASSERT_TRUE(RocksdbConfigOptions::getKVPaths("aaa", "aaa", kv_paths));
    ASSERT_EQ(kv_paths.size(), 1);
}

TEST(RocksdbEngineOptionsTest, memtableTest) {
    std::vector<std::pair<std::string, std::string>> mem_facs = {
            {"nullptr", "SkipListFactory"},  // nullptr is a default factory.
            {"skiplist", "SkipListFactory"},
            {"vector", "VectorRepFactory"},
            {"hashskiplist", "HashSkipListRepFactory"},
            {"hashlinklist", "HashLinkListRepFactory"},
            {"cuckoo", "HashCuckooRepFactory"}
    };
    for (auto mem_kind : mem_facs) {
        FLAGS_memtable_factory = mem_kind.first;
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        std::unique_ptr<RocksdbEngine> engine = std::make_unique<RocksdbEngine>(0,
                KV_DATA_PATH_FORMAT(rootPath.path(), 0),
                KV_WAL_PATH_FORMAT(rootPath.path(), 0));
        ASSERT(engine);
    }

    do {
        FLAGS_memtable_factory = "ccc";
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        rocksdb::Status s =
                std::make_shared<RocksdbConfigOptions>()->createRocksdbEngineOptions(false, false);
        ASSERT_EQ("Operation aborted: rocksdb option memtable_factory error", s.ToString());
    } while (false);
    FLAGS_memtable_factory = "nullptr";
}

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
