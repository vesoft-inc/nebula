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
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    int nebula_space_num = 2;

    KV_paths kv_paths;
    ASSERT_TRUE(RocksdbConfigOptions::getKVPaths(
            folly::stringPrintf("%s/disk1", rootPath.path()),
            folly::stringPrintf("%s/disk1", rootPath.path()),
            kv_paths));

    rocksdb::Status s = std::make_shared<RocksdbConfigOptions>(
            kv_paths,
            nebula_space_num,
            false,
            false)->createRocksdbEngineOptions(options);
    FLAGS_rocksdb_options_version = "5.15.10";
    ASSERT_EQ(rocksdb::Status::kNotSupported , s.code());
}

TEST(RocksdbEngineOptionsTest, simpleOptionTest) {
    rocksdb::Options options;
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    FLAGS_stats_dump_period_sec = "200";
    int nebula_space_num = 1;
    KV_paths kv_paths;
    ASSERT_TRUE(RocksdbConfigOptions::getKVPaths(
            folly::stringPrintf("%s/disk1", rootPath.path()),
            folly::stringPrintf("%s/disk1", rootPath.path()),
            kv_paths));
    rocksdb::Status s = std::make_shared<RocksdbConfigOptions>(
            kv_paths,
            nebula_space_num,
            false,
            false)->createRocksdbEngineOptions(options);
    LOG(INFO) << s.ToString();
    ASSERT_TRUE(s.ok());
    std::unique_ptr<RocksdbEngine> engine =
            std::make_unique<RocksdbEngine>(0, rootPath.path(), options);
    engine.reset(nullptr);

    ASSERT_FALSE(nebula::fs::FileUtils::fileType(rootPath.path()) ==
    nebula::fs::FileType::NOTEXIST);
    ASSERT_EQ(nebula::fs::FileUtils::fileType(rootPath.path()), nebula::fs::FileType::DIRECTORY);

    rocksdb::DBOptions loaded_db_opt;
    std::vector<rocksdb::ColumnFamilyDescriptor> loaded_cf_descs;

    s = LoadLatestOptions(rootPath.path(), rocksdb::Env::Default(), &loaded_db_opt,
                          &loaded_cf_descs);
    ASSERT_TRUE(s.ok());

    ASSERT_EQ(loaded_db_opt.stats_dump_period_sec,
            static_cast<int>(strtol(FLAGS_stats_dump_period_sec.c_str(), NULL, 10)));
}

TEST(RocksdbEngineOptionsTest, createOptionsTest) {
    rocksdb::Options options;
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    FLAGS_stats_dump_period_sec = "aaaaa";
    int nebula_space_num = 1;
    KV_paths kv_paths;
    ASSERT_TRUE(RocksdbConfigOptions::getKVPaths(
            folly::stringPrintf("%s/disk1", rootPath.path()),
            folly::stringPrintf("%s/disk1", rootPath.path()),
            kv_paths));
    rocksdb::Status s = std::make_shared<RocksdbConfigOptions>(
            kv_paths,
            nebula_space_num,
            false,
            false)->createRocksdbEngineOptions(options);
    FLAGS_stats_dump_period_sec = "600";
    ASSERT_EQ(rocksdb::Status::kInvalidArgument , s.code());
    ASSERT_EQ("Invalid argument: Unable to parse DBOptions:: stats_dump_period_sec",
            s.ToString());
}

TEST(RocksdbEngineOptionsTest, getOptionValueTest) {
    rocksdb::Options options;
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    FLAGS_stats_dump_period_sec = "200";
    int nebula_space_num = 1;
    KV_paths kv_paths;
    ASSERT_TRUE(RocksdbConfigOptions::getKVPaths(
            folly::stringPrintf("%s/disk1", rootPath.path()),
            folly::stringPrintf("%s/disk1", rootPath.path()),
            kv_paths));
    rocksdb::Status s = std::make_shared<RocksdbConfigOptions>(
            kv_paths,
            nebula_space_num,
            false,
            false)->createRocksdbEngineOptions(options);
    std::string value;
    ASSERT_FALSE(RocksdbConfigOptions::getRocksdbEngineOptionValue(
            RocksdbConfigOptions::ROCKSDB_OPTION_TYPE::DBOptions,
            FLAGS_stats_dump_period_sec.c_str(),
            value));
    ASSERT_TRUE(RocksdbConfigOptions::getRocksdbEngineOptionValue(
            RocksdbConfigOptions::ROCKSDB_OPTION_TYPE::DBOptions,
            "stats_dump_period_sec",
            value));
    ASSERT_EQ(FLAGS_stats_dump_period_sec, value);
    FLAGS_stats_dump_period_sec = "600";
}

TEST(RocksdbEngineOptionsTest, pathsTest) {
    fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
    KV_paths kv_paths;
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
        rocksdb::Options options;
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        int nebula_space_num = 1;
        nebula::kvstore::KV_paths kv_paths;
        ASSERT_TRUE(RocksdbConfigOptions::getKVPaths(
                folly::stringPrintf("%s/disk1", rootPath.path()),
                folly::stringPrintf("%s/disk1", rootPath.path()),
                kv_paths));
        rocksdb::Status s = std::make_shared<RocksdbConfigOptions>(
                kv_paths,
                nebula_space_num,
                false,
                false)->createRocksdbEngineOptions(options);
        ASSERT_TRUE(s.ok());
        std::unique_ptr<RocksdbEngine> engine =
                std::make_unique<RocksdbEngine>(0, rootPath.path(), options);
        ASSERT(engine);
        ASSERT_EQ(mem_kind.second, options.memtable_factory.get()->Name());
    }

    do {
        FLAGS_memtable_factory = "ccc";
        rocksdb::Options options;
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        int nebula_space_num = 1;
        KV_paths kv_paths;
        ASSERT_TRUE(RocksdbConfigOptions::getKVPaths(
                folly::stringPrintf("%s/disk1", rootPath.path()),
                folly::stringPrintf("%s/disk1", rootPath.path()),
                kv_paths));
        rocksdb::Status s = std::make_shared<RocksdbConfigOptions>(
                kv_paths,
                nebula_space_num,
                false,
                false)->createRocksdbEngineOptions(options);
        ASSERT_FALSE(s.ok());
        ASSERT_EQ("Operation aborted: rocksdb option memtable_factory error", s.ToString());
    } while (false);
    FLAGS_memtable_factory = "nullptr";
}

TEST(RocksdbEngineOptionsTest, compatibilityTest) {
    rocksdb::Options options;
}
}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
