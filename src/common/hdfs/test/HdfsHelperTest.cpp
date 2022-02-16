/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>      // for TestPartResult
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult

#include <memory>  // for allocator

#include "common/base/Logging.h"            // for SetStderrLogging
#include "common/base/Status.h"             // for operator<<
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/hdfs/HdfsCommandHelper.h"  // for HdfsCommandHelper

namespace nebula {
namespace hdfs {

TEST(HdfsHelper, ls) {
  auto helper = HdfsCommandHelper();
  auto result = helper.ls("127.0.0.1", 9000, "/");
  ASSERT_TRUE(result.ok()) << result.status();
}

TEST(HdfsHelper, copyToLocal) {
  auto helper = HdfsCommandHelper();
  auto result = helper.copyToLocal("127.0.0.1", 9000, "/", "/tmp");
  ASSERT_TRUE(result.ok()) << result.status();
}

}  // namespace hdfs
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
