/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include "kvstore/RocksdbEngine.h"
#include "fs/TempFile.h"
#include "fs/TempDir.h"

namespace nebula {
namespace kvstore {
TEST(Load, SSTLoad) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    fs::TempDir rootPath("/tmp/rocksdb_engine_test.XXXXXX");
    auto file = folly::stringPrintf("%s/%s", rootPath.path(), "data.sst");
    auto s = writer.Open(file);
    ASSERT_TRUE(s.ok());

    writer.Put("key", "value");
    writer.Finish();

    auto engine = std::make_unique<RocksdbEngine>(0, KV_DATA_PATH_FORMAT(rootPath.path(), 0),
            KV_WAL_PATH_FORMAT(rootPath.path(), 0));
    std::vector<std::string> files = {file};
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->ingest(files));

    std::string result;
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->get("key", &result));
    EXPECT_EQ(result, "value");
}
}   // namespace kvstore
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
