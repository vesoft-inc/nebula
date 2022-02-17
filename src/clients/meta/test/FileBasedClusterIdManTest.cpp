/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>     // for stringPrintf
#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>

#include <memory>  // for allocator

#include "clients/meta/FileBasedClusterIdMan.h"  // for FileBasedClusterIdMan
#include "common/base/Logging.h"                 // for Check_EQImpl, Check_...
#include "common/fs/TempDir.h"                   // for TempDir

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
