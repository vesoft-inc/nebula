/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>        // for init
#include <gflags/gflags_declare.h>  // for DECLARE_uint32
#include <glog/logging.h>           // for INFO
#include <gtest/gtest.h>            // for TestPartResult
#include <gtest/gtest.h>            // for Message
#include <gtest/gtest.h>            // for TestPartResult
#include <stdint.h>                 // for int64_t

#include <memory>  // for allocator

#include "common/base/Logging.h"    // for SetStderrLogging
#include "common/time/WallClock.h"  // for WallClock
#include "kvstore/RateLimiter.h"    // for RateLimiter

DECLARE_uint32(snapshot_part_rate_limit);

namespace nebula {
namespace kvstore {

TEST(RateLimiter, ConsumeLessEqualThanBurst) {
  RateLimiter limiter;
  auto now = time::WallClock::fastNowInSec();
  int64_t count = 0;
  while (count++ < 50) {
    limiter.consume(FLAGS_snapshot_part_rate_limit / 10,  // toConsume
                    FLAGS_snapshot_part_rate_limit,       // rate
                    FLAGS_snapshot_part_rate_limit);      // burstSize
  }
  EXPECT_GE(time::WallClock::fastNowInSec() - now, 5);
}

TEST(RateLimiter, ConsumeGreaterThanBurst) {
  RateLimiter limiter;
  auto now = time::WallClock::fastNowInSec();
  int64_t count = 0;
  while (count++ < 5) {
    // greater than burst size, will sleep 1 second instead
    limiter.consume(FLAGS_snapshot_part_rate_limit,        // toConsume
                    FLAGS_snapshot_part_rate_limit,        // rate
                    FLAGS_snapshot_part_rate_limit / 10);  // burstSize
  }
  EXPECT_GE(time::WallClock::fastNowInSec() - now, 5);
}

TEST(RateLimiter, RateLessThanBurst) {
  RateLimiter limiter;
  auto now = time::WallClock::fastNowInSec();
  int64_t count = 0;
  while (count++ < 5) {
    limiter.consume(FLAGS_snapshot_part_rate_limit,       // toConsume
                    FLAGS_snapshot_part_rate_limit,       // rate
                    2 * FLAGS_snapshot_part_rate_limit);  // burstSize
  }
  EXPECT_GE(time::WallClock::fastNowInSec() - now, 5);
}

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
