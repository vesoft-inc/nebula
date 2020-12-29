/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "common/time/TimeUtils.h"

namespace nebula {

TEST(Time, secondsTimeCovertion) {
    // DateTime
    {
        std::vector<DateTime> values;
        values.emplace_back(DateTime(1950, 1, 2, 0, 22, 3, 0));
        for (std::size_t i = 0; i < 4; ++i) {
            values.emplace_back(
                DateTime(folly::Random::rand32(1000, std::numeric_limits<int16_t>::max()),
                         folly::Random::rand32(1, 13),
                         folly::Random::rand32(1, 29),
                         folly::Random::rand32(0, 24),
                         folly::Random::rand32(0, 60),
                         folly::Random::rand32(0, 60),
                         0));
        }
        for (const auto &dt : values) {
            EXPECT_EQ(
                dt,
                time::TimeUtils::unixSecondsToDateTime(time::TimeUtils::dateTimeToUnixSeconds(dt)));
        }
    }
    {
        std::vector<std::pair<DateTime, int64_t>> dateTimeSeconds{
            {{DateTime(1930, 3, 4, 3, 40, 5, 0), -1256933995},
             {DateTime(1970, 1, 1, 0, 0, 0, 0), 0},
             {DateTime(2020, 9, 17, 1, 35, 18, 0), 1600306518}},
        };
        for (std::size_t i = 0; i < dateTimeSeconds.size(); ++i) {
            EXPECT_EQ(dateTimeSeconds[i].second,
                      time::TimeUtils::dateTimeToUnixSeconds(dateTimeSeconds[i].first));
            EXPECT_EQ(time::TimeUtils::unixSecondsToDateTime(dateTimeSeconds[i].second),
                      dateTimeSeconds[i].first);
        }
    }
    // Date
    {
        std::vector<Date> values;
        values.emplace_back(Date(1950, 1, 2));
        for (std::size_t i = 0; i < 4; ++i) {
            values.emplace_back(
                Date(folly::Random::rand32(1000, std::numeric_limits<int16_t>::max()),
                     folly::Random::rand32(1, 13),
                     folly::Random::rand32(1, 29)));
        }
        for (const auto &d : values) {
            EXPECT_EQ(d, time::TimeUtils::unixSecondsToDate(time::TimeUtils::dateToUnixSeconds(d)));
        }
    }
    {
        std::vector<std::pair<Date, int64_t>> dateSeconds{{{Date(1930, 3, 4), -1256947200},
                                                           {Date(1970, 1, 1), 0},
                                                           {Date(2003, 3, 4), 1046736000}}};
        for (std::size_t i = 0; i < dateSeconds.size(); ++i) {
            EXPECT_EQ(dateSeconds[i].second,
                      time::TimeUtils::dateToUnixSeconds(dateSeconds[i].first));
            EXPECT_EQ(time::TimeUtils::unixSecondsToDate(dateSeconds[i].second),
                      dateSeconds[i].first);
        }
    }
    // Time
    {
        std::vector<Time> values;
        for (std::size_t i = 0; i < 4; ++i) {
            values.emplace_back(Time(folly::Random::rand32(0, 24),
                                     folly::Random::rand32(0, 60),
                                     folly::Random::rand32(0, 60),
                                     0));
        }
        for (const auto &t : values) {
            EXPECT_EQ(t, time::TimeUtils::unixSecondsToTime(time::TimeUtils::timeToSeconds(t)));
        }
    }
    {
        std::vector<std::pair<Time, int64_t>> timeSeconds{
            {{Time(0, 0, 0, 0), 0}, {Time(14, 2, 4, 0), 50524}}};
        for (std::size_t i = 0; i < timeSeconds.size(); ++i) {
            EXPECT_EQ(timeSeconds[i].second, time::TimeUtils::timeToSeconds(timeSeconds[i].first));
            EXPECT_EQ(time::TimeUtils::unixSecondsToTime(timeSeconds[i].second),
                      timeSeconds[i].first);
        }
    }
    // Timestamp
    {
        // incorrect type
        EXPECT_FALSE(time::TimeUtils::toTimestamp(Value(10.0)).ok());
        // incorrect int value
        EXPECT_FALSE(time::TimeUtils::toTimestamp(std::numeric_limits<int64_t>::max()-10).ok());
        // incorrect string value
        EXPECT_FALSE(time::TimeUtils::toTimestamp(Value("2001-02-29T10:00:10")).ok());
        // incorrect string value
        EXPECT_FALSE(time::TimeUtils::toTimestamp(Value("2001-04-31T10:00:10")).ok());
        // correct int
        EXPECT_TRUE(time::TimeUtils::toTimestamp(Value(0)).ok());
        // correct int
        EXPECT_TRUE(time::TimeUtils::toTimestamp(
                Value(std::numeric_limits<int64_t>::max() / 1000000000)).ok());
        // correct string
        EXPECT_TRUE(time::TimeUtils::toTimestamp("2020-08-01T09:00:00").ok());
    }
}

}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    auto result = nebula::time::TimeUtils::initializeGlobalTimezone();
    if (!result.ok()) {
        LOG(FATAL) << result;
    }

    DLOG(INFO) << "Timezone: " << nebula::time::TimeUtils::getGlobalTimezone().stdZoneName();
    DLOG(INFO) << "Timezone offset: "
               << nebula::time::TimeUtils::getGlobalTimezone().utcOffsetSecs();

    return RUN_ALL_TESTS();
}
