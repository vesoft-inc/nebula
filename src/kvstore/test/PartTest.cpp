/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "kvstore/Part.h"

namespace nebula {
namespace kvstore {

TEST(PartTest, RocksTest) {
    fs::TempDir dataPath("/tmp/rocksdb_test.XXXXXX");
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, dataPath.path(), &db);
    CHECK(status.ok());
    LOG(INFO) << "Write data into rocksdb...";
    rocksdb::WriteBatch updates;
    std::vector<KV> kvs;
    for (uint32_t i = 0; i < 1000; i++) {
        kvs.emplace_back(folly::stringPrintf("key%d", i),
                         folly::stringPrintf("val%d", i));
    }
    for (auto& kv : kvs) {
        updates.Put(rocksdb::Slice(kv.first), rocksdb::Slice(kv.second));
    }
    rocksdb::WriteOptions woptions;
    status = db->Write(woptions, &updates);
    CHECK(status.ok());
    LOG(INFO) << "Read data from rocksdb...";
    rocksdb::ReadOptions roptions;
    for (uint32_t i = 0; i < 1000; i++) {
        std::string val;
        status = db->Get(roptions, rocksdb::Slice(folly::stringPrintf("key%d", i)), &val);
        CHECK(status.ok());
        EXPECT_EQ(folly::stringPrintf("val%d", i), val);
    }

    LOG(INFO) << "Test finished...";
    delete db;
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

