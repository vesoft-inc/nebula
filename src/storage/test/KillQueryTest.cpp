#include <gtest/gtest.h>

#include "common/fs/TempDir.h"
#include "mock/MockCluster.h"
namespace nebula {
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
namespace storage {

TEST(KillQuery, GetNeighbors) {
  fs::TempDir rootPath("/tmp/KVSimpleTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
}
TEST(KillQuery, LookUpEdge) {}
TEST(KillQuery, LookUpVertex) {}
}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
