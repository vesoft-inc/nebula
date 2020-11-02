/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/time/TimezoneInfo.h"

TEST(TimezoneInfo, PosixTimezone) {
    {
        const std::string posixTimezone =
            "EST-05:00:00EDT+01:00:00,M4.1.0/02:00:00,M10.5.0/02:00:00";
        nebula::time::Timezone tz;
        ASSERT_TRUE(tz.parsePosixTimezone(posixTimezone).ok());

        // std zonename
        EXPECT_EQ("EST", tz.stdZoneName());

        // std offset
        EXPECT_EQ(-5 * 60 * 60, tz.utcOffsetSecs());
    }
    {
        // short case
        const std::string posixTimezone = "EST-05:00:00";
        nebula::time::Timezone tz;
        ASSERT_TRUE(tz.parsePosixTimezone(posixTimezone).ok());

        // std zonename
        EXPECT_EQ("EST", tz.stdZoneName());

        // std offset
        EXPECT_EQ(-5 * 60 * 60, tz.utcOffsetSecs());
    }
}

TEST(TimezoneInfo, PosixTimezoneInvalid) {
    const std::string posixTimezone = "233333333333";
    nebula::time::Timezone tz;
    ASSERT_FALSE(tz.parsePosixTimezone(posixTimezone).ok());
}


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
