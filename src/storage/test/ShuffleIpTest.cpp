/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/IPAddress.h>  // for IPAddress
#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>      // for TestPartResult
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult
#include <stddef.h>           // for size_t

#include <string>  // for string, basic_string, allocator
#include <vector>  // for vector

#include "common/base/Logging.h"  // for SetStderrLogging

namespace nebula {
namespace storage {

TEST(ShuffleIpTest, validateTest) {
  std::vector<std::string> addrs;
  addrs.emplace_back("hp-server");
  addrs.emplace_back("nebula-dev-1");
  addrs.emplace_back("192.168.8.5");

  std::vector<bool> result;
  for (size_t i = 0; i < addrs.size(); ++i) {
    result.push_back(folly::IPAddress::validate(addrs[i]));
  }

  EXPECT_FALSE(result[0]);
  EXPECT_FALSE(result[1]);
  EXPECT_TRUE(result[2]);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
