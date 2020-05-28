/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/base/SlowOpTracker.h"
#include <gtest/gtest.h>

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
