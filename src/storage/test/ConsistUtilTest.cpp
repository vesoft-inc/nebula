/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/container/Enumerate.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "folly/String.h"
#include "storage/transaction/ConsistUtil.h"

#define LOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

namespace nebula {
namespace storage {

TEST(ConsistUtilTest, primeTable) {}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
