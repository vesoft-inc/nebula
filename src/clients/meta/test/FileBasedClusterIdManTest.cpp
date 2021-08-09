/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/fs/TempDir.h"
#include "common/clients/meta/FileBasedClusterIdMan.h"

namespace nebula {
namespace meta {

TEST(FileBasedClusterIdManTest, ReadWriteTest) {
    fs::TempDir rootPath("/tmp/FileBasedClusterIdManTest.XXXXXX");
    auto clusterId = FileBasedClusterIdMan::create("127.0.0.1:44500");
    CHECK_NE(0, clusterId);
    auto file = folly::stringPrintf("%s/cluster.id", rootPath.path());
    CHECK(FileBasedClusterIdMan::persistInFile(clusterId, file));
    auto ret = FileBasedClusterIdMan::getClusterIdFromFile(file);
    CHECK_EQ(clusterId, ret);
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
