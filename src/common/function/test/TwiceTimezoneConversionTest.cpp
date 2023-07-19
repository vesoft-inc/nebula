// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include <gtest/gtest.h>

#include "common/datatypes/Date.h"
#include "common/function/FunctionManager.h"
#include "common/time/TimezoneInfo.h"

DECLARE_string(timezone_name);

// #4432
TEST(DatetimeFunction, TwiceTimezoneConversion) {
  // date
  {
    auto result = nebula::FunctionManager::get("date", 1);
    ASSERT_TRUE(result.ok());
    nebula::Value arg = nebula::Value("2019-01-01");
    auto res = std::move(result).value()({arg});
    ASSERT_EQ(res.type(), nebula::Value::Type::DATE);
    EXPECT_EQ(res.getDate(), nebula::Date(2019, 1, 1));
  }

  // time
  {
    auto result = nebula::FunctionManager::get("time", 1);
    ASSERT_TRUE(result.ok());
    nebula::Value arg = nebula::Value("23:04:05+08:00");
    auto res = std::move(result).value()({arg});
    ASSERT_EQ(res.type(), nebula::Value::Type::TIME);
    EXPECT_EQ(res.getTime(), nebula::Time(15, 4, 5, 0));
  }

  // datetime
  {
    auto result = nebula::FunctionManager::get("datetime", 1);
    ASSERT_TRUE(result.ok());
    nebula::Value arg = nebula::Value("2019-01-01T23:04:05+08:00");
    auto res = std::move(result).value()({arg});
    ASSERT_EQ(res.type(), nebula::Value::Type::DATETIME);
    EXPECT_EQ(res.getDateTime(), nebula::DateTime(2019, 1, 1, 15, 4, 5, 0));
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  FLAGS_timezone_name = "UTC+08:00";

  auto result = nebula::time::Timezone::initializeGlobalTimezone();
  if (!result.ok()) {
    LOG(FATAL) << result;
  }

  DLOG(INFO) << "Timezone: " << nebula::time::Timezone::getGlobalTimezone().stdZoneName();
  DLOG(INFO) << "Timezone offset: " << nebula::time::Timezone::getGlobalTimezone().utcOffsetSecs();

  return RUN_ALL_TESTS();
}
