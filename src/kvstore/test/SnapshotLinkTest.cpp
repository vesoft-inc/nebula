/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/fs/TempDir.h"
#include <fstream>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/utilities/checkpoint.h>
#include <folly/Benchmark.h>

namespace nebula {
namespace kvstore {
TEST(SnapshotLinkTest, SimpleTest) {
    int64_t log_size_for_flush = 0;
    rocksdb::Options options;
    rocksdb::DB* db;
    rocksdb::DB* sdb;
    std::string result;
    rocksdb::Checkpoint* checkpoint;

    fs::TempDir dbDir("/tmp/snapshot_test_dbname.XXXXXX");
    fs::TempDir snapshot("/tmp/snapshot_test_snapshot.XXXXXX");
    fs::TempDir snapshotCopy("/tmp/snapshot_test_snapshot_copy.XXXXXX");

    std::string snapshotDir = folly::stringPrintf("%s/snapshot", snapshot.path());

    // Create a database
    options.create_if_missing = true;
    ASSERT_TRUE(rocksdb::DB::Open(options, dbDir.path(), &db).ok());
    std::string key = std::string("foo");
    ASSERT_TRUE(db->Put(rocksdb::WriteOptions(), key, "v1").ok());

    // Take a snapshot
    ASSERT_TRUE(rocksdb::Checkpoint::Create(db, &checkpoint).ok());
    ASSERT_TRUE(checkpoint->CreateCheckpoint(snapshotDir, log_size_for_flush).ok());
    ASSERT_TRUE(db->Put(rocksdb::WriteOptions(), key, "v2").ok());
    ASSERT_TRUE(db->Get(rocksdb::ReadOptions(), key, &result).ok());
    ASSERT_EQ("v2", result);
    ASSERT_TRUE(db->Flush(rocksdb::FlushOptions()).ok());
    ASSERT_TRUE(db->Get(rocksdb::ReadOptions(), key, &result).ok());
    ASSERT_EQ("v2", result);
    // Open snapshot and verify contents while DB is running
    options.create_if_missing = false;
    ASSERT_TRUE(rocksdb::DB::Open(options, snapshotDir, &sdb).ok());
    ASSERT_TRUE(sdb->Get(rocksdb::ReadOptions(), key, &result).ok());
    ASSERT_EQ("v1", result);
    delete sdb;
    sdb = nullptr;
    delete db;
    db = nullptr;

    // Destroy original DB
    ASSERT_TRUE(DestroyDB(dbDir.path(), options).ok());

    // Open snapshot and verify contents
    options.create_if_missing = false;
    ASSERT_TRUE(rocksdb::DB::Open(options, snapshotDir, &db).ok());
    ASSERT_TRUE(db->Get(rocksdb::ReadOptions(), key, &result).ok());
    ASSERT_EQ("v1", result);
    delete db;
    db = nullptr;
    {
        auto files = fs::FileUtils::listAllFilesInDir(snapshotDir.data(), false, "*");
        for (const auto &file : files) {
            std::fstream srcF(folly::stringPrintf("%s/%s", snapshotDir.data(), file.data()),
                              std::ios::binary | std::ios::in);
            std::fstream destF(folly::stringPrintf("%s/%s", snapshotCopy.path(), file.data()),
                               std::ios::binary | std::ios::out);
            destF << srcF.rdbuf();
            destF.close();
            srcF.close();
        }
    }
    ASSERT_TRUE(DestroyDB(snapshotDir, options).ok());

    // Verify snapshot after snapshot folder was moved.
    options.create_if_missing = false;
    ASSERT_TRUE(rocksdb::DB::Open(options, snapshotCopy.path(), &db).ok());
    ASSERT_TRUE(db->Get(rocksdb::ReadOptions(), key, &result).ok());
    ASSERT_EQ("v1", result);
    delete db;
    ASSERT_TRUE(DestroyDB(snapshotCopy.path(), options).ok());
    delete checkpoint;
}
}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

