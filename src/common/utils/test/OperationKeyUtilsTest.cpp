/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult

#include <memory>  // for allocator

#include "common/base/Logging.h"             // for SetStderrLogging
#include "common/thrift/ThriftTypes.h"       // for PartitionID
#include "common/utils/OperationKeyUtils.h"  // for OperationKeyUtils

namespace nebula {

TEST(OperationKeyUtilsTest, ModifyKeyTest) {
  PartitionID part = 1;
  auto opKey = OperationKeyUtils::modifyOperationKey(part, "modify key");
  ASSERT_TRUE(OperationKeyUtils::isModifyOperation(opKey));
  ASSERT_EQ(OperationKeyUtils::getOperationKey(opKey), "modify key");
}

TEST(OperationKeyUtilsTest, DeleteKeyTest) {
  PartitionID part = 1;
  auto opKey = OperationKeyUtils::deleteOperationKey(part);
  ASSERT_TRUE(OperationKeyUtils::isDeleteOperation(opKey));
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
