/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/time/TimeUtils.h"
#include "common/time/TimezoneInfo.h"

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
            EXPECT_EQ(dt,
                      time::TimeConversion::unixSecondsToDateTime(
                          time::TimeConversion::dateTimeToUnixSeconds(dt)));
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
                      time::TimeConversion::dateTimeToUnixSeconds(dateTimeSeconds[i].first));
            EXPECT_EQ(time::TimeConversion::unixSecondsToDateTime(dateTimeSeconds[i].second),
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
            EXPECT_EQ(d,
                      time::TimeConversion::unixSecondsToDate(
                          time::TimeConversion::dateToUnixSeconds(d)));
        }
    }
    {
        std::vector<std::pair<Date, int64_t>> dateSeconds{{{Date(1930, 3, 4), -1256947200},
                                                           {Date(1970, 1, 1), 0},
                                                           {Date(2003, 3, 4), 1046736000}}};
        for (std::size_t i = 0; i < dateSeconds.size(); ++i) {
            EXPECT_EQ(dateSeconds[i].second,
                      time::TimeConversion::dateToUnixSeconds(dateSeconds[i].first));
            EXPECT_EQ(time::TimeConversion::unixSecondsToDate(dateSeconds[i].second),
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
            EXPECT_EQ(
                t, time::TimeConversion::unixSecondsToTime(time::TimeConversion::timeToSeconds(t)));
        }
    }
    {
        std::vector<std::pair<Time, int64_t>> timeSeconds{
            {{Time(0, 0, 0, 0), 0}, {Time(14, 2, 4, 0), 50524}}};
        for (std::size_t i = 0; i < timeSeconds.size(); ++i) {
            EXPECT_EQ(timeSeconds[i].second,
                      time::TimeConversion::timeToSeconds(timeSeconds[i].first));
            EXPECT_EQ(time::TimeConversion::unixSecondsToTime(timeSeconds[i].second),
                      timeSeconds[i].first);
        }
    }
    // Timestamp
    {
        // incorrect type
        EXPECT_FALSE(time::TimeUtils::toTimestamp(Value(10.0)).ok());
        // incorrect int value
        EXPECT_FALSE(time::TimeUtils::toTimestamp(std::numeric_limits<int64_t>::max() - 10).ok());
        // incorrect string value
        EXPECT_FALSE(time::TimeUtils::toTimestamp(Value("2001-02-29T10:00:10")).ok());
        // incorrect string value
        EXPECT_FALSE(time::TimeUtils::toTimestamp(Value("2001-04-31T10:00:10")).ok());
        // correct int
        EXPECT_TRUE(time::TimeUtils::toTimestamp(Value(0)).ok());
        // correct int
        EXPECT_TRUE(
            time::TimeUtils::toTimestamp(Value(std::numeric_limits<int64_t>::max() / 1000000000))
                .ok());
        // correct string
        EXPECT_TRUE(time::TimeUtils::toTimestamp("2020-08-01T09:00:00").ok());
    }
}

TEST(Time, TimezoneShift) {
    // time
    {
        Time t(0, 0, 0, 30);
        auto nt = time::TimeConversion::timeShift(t, -10);
        EXPECT_EQ(nt, Time(23, 59, 50, 30));
    }
    {
        Time t(1, 0, 0, 30);
        auto nt = time::TimeConversion::timeShift(t, -10);
        EXPECT_EQ(nt, Time(0, 59, 50, 30));
    }
    {
        Time t(23, 59, 55, 30);
        auto nt = time::TimeConversion::timeShift(t, 10);
        EXPECT_EQ(nt, Time(0, 0, 5, 30));
    }
    {
        Time t(23, 59, 30, 30);
        auto nt = time::TimeConversion::timeShift(t, 10);
        EXPECT_EQ(nt, Time(23, 59, 40, 30));
    }
    // datetime
    {
        DateTime dt(2001, 3, 24, 0, 0, 5, 30);
        auto nt = time::TimeConversion::dateTimeShift(dt, -10);
        EXPECT_EQ(nt, DateTime(2001, 3, 23, 23, 59, 55, 30));
    }
    {
        DateTime dt(2001, 3, 24, 0, 0, 15, 30);
        auto nt = time::TimeConversion::dateTimeShift(dt, -10);
        EXPECT_EQ(nt, DateTime(2001, 3, 24, 0, 0, 5, 30));
    }
    {
        DateTime dt(2001, 3, 24, 23, 59, 55, 30);
        auto nt = time::TimeConversion::dateTimeShift(dt, 10);
        EXPECT_EQ(nt, DateTime(2001, 3, 25, 0, 0, 5, 30));
    }
    {
        DateTime dt(2001, 3, 24, 3, 40, 30, 30);
        auto nt = time::TimeConversion::dateTimeShift(dt, 10);
        EXPECT_EQ(nt, DateTime(2001, 3, 24, 3, 40, 40, 30));
    }
}

TEST(Time, Parse) {
    // datetime
    {
        auto result = time::TimeUtils::parseDateTime("2019-03-04 22:00:30");
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), DateTime(2019, 3, 4, 22, 0, 30, 0));
    }
    {
        auto result = time::TimeUtils::parseDateTime("2019-03-04T22:00:30");
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(result.value(), DateTime(2019, 3, 4, 22, 0, 30, 0));
    }
}

}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    auto result = nebula::time::Timezone::initializeGlobalTimezone();
    if (!result.ok()) {
        LOG(FATAL) << result;
    }

    DLOG(INFO) << "Timezone: " << nebula::time::Timezone::getGlobalTimezone().stdZoneName();
    DLOG(INFO) << "Timezone offset: "
               << nebula::time::Timezone::getGlobalTimezone().utcOffsetSecs();

    return RUN_ALL_TESTS();
}
