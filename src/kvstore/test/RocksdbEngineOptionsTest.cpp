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

    ASSERT_EQ(loadedDbOpt.stats_dump_period_sec,
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

TEST(RocksdbEngineOptionsTest, memtableTest) {
    std::vector<std::pair<std::string, std::string>> memFacs = {
            {"nullptr", "SkipListFactory"},  // nullptr is a default factory.
            {"skiplist", "SkipListFactory"},
            {"vector", "VectorRepFactory"},
            {"hashskiplist", "HashSkipListRepFactory"},
            {"hashlinklist", "HashLinkListRepFactory"},
            {"cuckoo", "HashCuckooRepFactory"}
    };
    for (auto memKind : memFacs) {
        FLAGS_memtable_factory = memKind.first;
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        std::unique_ptr<RocksdbEngine> engine = std::make_unique<RocksdbEngine>(0,
                KV_DATA_PATH_FORMAT(rootPath.path(), 0));
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

TEST(RocksdbEngineOptionsTest, compactionFilterTest) {
    std::vector<std::string> compactionFilters = {
            {""},
            {"nullptr"},
            {"nebula"}
    };
    for (auto compactionFilter : compactionFilters) {
        FLAGS_compaction_filter = compactionFilter;
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        std::unique_ptr<RocksdbEngine> engine = std::make_unique<RocksdbEngine>(0,
                 KV_DATA_PATH_FORMAT(rootPath.path(), 0));
        ASSERT(engine);
    }
    do {
        FLAGS_compaction_filter = "ccc";
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        rocksdb::Status s =
                std::make_shared<RocksdbConfigOptions>()->createRocksdbEngineOptions(false, false);
        ASSERT_EQ("Operation aborted: rocksdb option compaction_filter error", s.ToString());
    } while (false);
    FLAGS_compaction_filter = "nullptr";
}

TEST(RocksdbEngineOptionsTest, compactionFilterFactoryTest) {
    std::vector<std::string> compactionFilterFactories = {
            {""},
            {"nullptr"},
            {"nebula"}
    };
    for (auto compactionFilterFactory : compactionFilterFactories) {
        FLAGS_compaction_filter_factory = compactionFilterFactory;
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        std::unique_ptr<RocksdbEngine> engine = std::make_unique<RocksdbEngine>(0,
                       KV_DATA_PATH_FORMAT(rootPath.path(), 0));
        ASSERT(engine);
    }
    do {
        FLAGS_compaction_filter_factory = "ccc";
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        rocksdb::Status s =
                std::make_shared<RocksdbConfigOptions>()->createRocksdbEngineOptions(false, false);
        ASSERT_EQ("Operation aborted: rocksdb option compaction_filter_factory error",
                s.ToString());
    } while (false);
        FLAGS_compaction_filter_factory = "nullptr";
}

TEST(RocksdbEngineOptionsTest, mergeOperatorTest) {
    std::vector<std::string> mergeOperators = {
            {""},
            {"nullptr"},
            {"nebula"}
    };
    for (auto mergeOperator : mergeOperators) {
        FLAGS_merge_operator = mergeOperator;
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        std::unique_ptr<RocksdbEngine> engine = std::make_unique<RocksdbEngine>(0,
                       KV_DATA_PATH_FORMAT(rootPath.path(), 0));
        ASSERT(engine);
    }
    do {
        FLAGS_merge_operator = "ccc";
        fs::TempDir rootPath("/tmp/kvstore_test.XXXXXX");
        rocksdb::Status s =
                std::make_shared<RocksdbConfigOptions>()->createRocksdbEngineOptions(false, false);
        ASSERT_EQ("Operation aborted: rocksdb option merge_operator error", s.ToString());
    } while (false);
        FLAGS_merge_operator = "nullptr";
}

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
