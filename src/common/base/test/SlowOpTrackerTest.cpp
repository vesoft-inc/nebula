/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <unistd.h>           // for usleep

#include <memory>  // for allocator

#include "common/base/Logging.h"        // for SetStderrLogging, CHECK, COMP...
#include "common/base/SlowOpTracker.h"  // for SlowOpTracker

namespace nebula {

TEST(SlowOpTrackerTest, SimpleTest) {
  SlowOpTracker tracker;
  usleep(500000);
  CHECK(tracker.slow());
  tracker.output("PREFIX", "This is a prefix msg");
}

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
