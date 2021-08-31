/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/time/TimeParser.h"

TEST(TimeParser, DateTime) {
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01-03T22:22:3.2333");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::DateTime(2019, 1, 3, 22, 22, 3, 233300), result.value());
  }
  // with offset
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01-03T22:22:3.2333+02:30");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::DateTime(2019, 1, 3, 19, 52, 3, 233300), result.value());
  }
  // lack day
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01T22:22:3.2333");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::DateTime(2019, 1, 1, 22, 22, 3, 233300), result.value());
  }
  // lack month
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019T22:22:3.2333");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::DateTime(2019, 1, 1, 22, 22, 3, 233300), result.value());
  }
  // lack us
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019T22:22:3");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::DateTime(2019, 1, 1, 22, 22, 3, 0), result.value());
  }
  // lack second
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019T22:22");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::DateTime(2019, 1, 1, 22, 22, 0, 0), result.value());
  }
  // lack second
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019T22");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::DateTime(2019, 1, 1, 22, 0, 0, 0), result.value());
  }
}

TEST(TimeParser, DateTimeFailed) {
  // out of range offset
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01-03T22:22:3.2333+24:30");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // only time
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("22:22:3.2333");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // with unexpected character
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01-03T22:22:3.2333x");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // not ending delimiter
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01-T22:22:3.2333");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01-03T22:22:.2333");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // not ending prefix
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01-03T");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("2019-01-03T22:22:3.");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // not exits prefix
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDateTime("-01-03T22:22:3.2333");
    EXPECT_FALSE(result.ok()) << result.status();
  }
}

TEST(TimeParser, Date) {
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDate("2019-01-03");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::Date(2019, 1, 3), result.value());
  }
  // lack day
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDate("2019-01");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::Date(2019, 1, 1), result.value());
  }
  // lack month
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDate("2019");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::Date(2019, 1, 1), result.value());
  }
}

TEST(TimeParser, DateFailed) {
  // don't support offset
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDate("2019-01-03+23:00");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // with unexpected character
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDate("2019-01-03*");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // extra components
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDate("2019-01-03T22:22:3.2333");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // not ending delimiter
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDate("2019-01-");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // not exits prefix
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseDate("-01-03");
    EXPECT_FALSE(result.ok()) << result.status();
  }
}

TEST(TimeParser, Time) {
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22:22:3.2333");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::Time(22, 22, 3, 233300), result.value());
  }
  // with offset
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22:22:3.2333-03:30");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::Time(1, 52, 3, 233300), result.value());
  }
  // lack us
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22:22:3");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::Time(22, 22, 3, 0), result.value());
  }
  // lack second
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22:22");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::Time(22, 22, 0, 0), result.value());
  }
  // lack minute
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22");
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(nebula::Time(22, 0, 0, 0), result.value());
  }
}

TEST(TimeParser, TimeFailed) {
  // out of range offset
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22:22:3.2333-03:60");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // unexpected character
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22:22:3.2333x");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // extra components
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("2019-01-03T22:22:3.2333");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // not ending delimiter
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22:22:.2333");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // not ending prefix
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime("22:22:3.");
    EXPECT_FALSE(result.ok()) << result.status();
  }
  // not exist prefix
  {
    nebula::time::TimeParser parser;
    auto result = parser.parseTime(":22:3.2333");
    EXPECT_FALSE(result.ok()) << result.status();
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
