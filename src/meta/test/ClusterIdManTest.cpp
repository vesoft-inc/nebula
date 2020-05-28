/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "meta/KVBasedClusterIdMan.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(ClusterIDManTest, FileTest) {
    fs::TempDir rootPath("/tmp/ClusterIDManTest.XXXXXX");
    auto clusterId = ClusterIdMan::create("127.0.0.1:44500");
    CHECK_NE(0, clusterId);
    auto file = folly::stringPrintf("%s/cluster.id", rootPath.path());
    CHECK(ClusterIdMan::persistInFile(clusterId, file));
    auto ret = ClusterIdMan::getClusterIdFromFile(file);
    CHECK_EQ(clusterId, ret);
}

TEST(ClusterIDManTest, KVTest) {
    fs::TempDir rootPath("/tmp/ClusterIDManTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    auto clusterId = ClusterIdMan::create("127.0.0.1:44500");
    CHECK_NE(0, clusterId);
    CHECK(ClusterIdMan::persistInKV(kv.get(), "clusterId", clusterId));
    auto ret = ClusterIdMan::getClusterIdFromKV(kv.get(), "clusterId");
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


