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

    writer.Put("key", "value");
    writer.Finish();

    auto engine = std::make_unique<RocksEngine>(0, rootPath.path());
    std::vector<std::string> files = {file};
    EXPECT_EQ(ResultCode::SUCCEEDED, engine->ingest(files));

    auto res = engine->get("key");
    EXPECT_TRUE(ok(res));
    EXPECT_EQ("value", value(std::move(res)));
}

}   // namespace kvstore
}   // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
