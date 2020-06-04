/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/time/WallClock.h"

using nebula::time::WallClock;

TEST(WallClock, TimePointInSeconds) {
    for (int i = 0; i < 100; i++) {
        auto tp1 = WallClock::slowNowInSec();
        auto tp2 = WallClock::fastNowInSec();
        auto tp3 = WallClock::slowNowInSec();

        // Allow one second off
        ASSERT_LE(tp1 - 1, tp2);
        ASSERT_LE(tp2, tp3 + 1);
    }
}


TEST(WallClock, TimePointInMilliSeconds) {
    for (int i = 0; i < 100; i++) {
        auto tp1 = WallClock::slowNowInMilliSec();
        auto tp2 = WallClock::fastNowInMilliSec();
        auto tp3 = WallClock::slowNowInMilliSec();

        // Allow 500 ms off
        ASSERT_LE(tp1 - 500, tp2);
        ASSERT_LE(tp2, tp3 + 500);
    }
}


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
