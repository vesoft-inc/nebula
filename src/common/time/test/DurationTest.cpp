/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>      // for TestPartResult
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult
#include <unistd.h>           // for sleep, usleep

#include <chrono>  // for milliseconds, duration_cast, seconds
#include <memory>  // for allocator

#include "common/base/Logging.h"   // for SetStderrLogging
#include "common/time/Duration.h"  // for Duration

using nebula::time::Duration;

TEST(Duration, elapsedInSeconds) {
  for (int i = 0; i < 5; i++) {
    Duration dur;
    auto start = std::chrono::steady_clock::now();
    sleep(2);
    auto diff = std::chrono::steady_clock::now() - start;
    dur.pause();

    ASSERT_EQ(std::chrono::duration_cast<std::chrono::seconds>(diff).count(), dur.elapsedInSec())
        << "Inaccuracy in iteration " << i;
  }
}

TEST(Duration, elapsedInMilliSeconds) {
  Duration dur;
  for (int i = 0; i < 200; i++) {
    dur.reset();
    auto start = std::chrono::steady_clock::now();
    usleep(5000);  // Sleep for 5 ms
    auto diff = std::chrono::steady_clock::now() - start;
    dur.pause();

    // Allow 1ms difference
    ASSERT_LE(std::chrono::duration_cast<std::chrono::milliseconds>(diff).count(),
              dur.elapsedInMSec())
        << "Inaccuracy in iteration " << i;
    ASSERT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(diff).count() + 1,
              dur.elapsedInMSec())
        << "Inaccuracy in iteration " << i;
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
