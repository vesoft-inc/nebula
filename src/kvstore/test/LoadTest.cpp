/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include "kvstore/RocksEngine.h"
#include "fs/TempFile.h"
#include "fs/TempDir.h"

namespace nebula {
namespace kvstore {

TEST(Load, SSTLoad) {
    rocksdb::Options options;
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    fs::TempDir rootPath("/tmp/load_test.XXXXXX");
    auto file = folly::stringPrintf("%s/%s", rootPath.path(), "data.sst");
    auto s = writer.Open(file);
    ASSERT_TRUE(s.ok());

    s = writer.Put("key", "value");
    ASSERT_TRUE(s.ok());
    s = writer.Put("key-empty", "");
    ASSERT_TRUE(s.ok());
    writer.Finish();

    auto engine = std::make_unique<RocksEngine>(0, rootPath.path());
    std::vector<std::string> files = {file};
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->ingest(files));

    std::string result;
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->get("key", &result));
    EXPECT_EQ("value", result);
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->get("key-empty", &result));
    EXPECT_EQ("", result);
    EXPECT_EQ(ResultCode::ERR_KEY_NOT_FOUND, engine->get("key-404", &result));
}

}   // namespace kvstore
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
