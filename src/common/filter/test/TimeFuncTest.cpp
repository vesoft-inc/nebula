/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "filter/TimeFunction.h"

namespace nebula {

class TimeFunctionTest : public testing::Test {
public:
    void SetUp() override {};

    void TearDown() override {};
};

TEST_F(TimeFunctionTest, Test_addDateFunc) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));
        args.push_back(int64_t(20));

        AddDateFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value()), "2019-12-28 05:10:00");
    }

    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));
        args.push_back(int64_t(20));
        args.push_back(int64_t(HOUR));

        AddDateFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value()), "2019-12-09 01:10:00");
    }

    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));
        args.push_back(int64_t(2));
        args.push_back(int64_t(MONTH));

        AddDateFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value()), "2020-02-08 05:10:00");
    }
}

TEST_F(TimeFunctionTest, Test_addTimeFunc) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-2-18 05:10:00"));
        args.push_back(std::string("20:10:00"));

        AddTimeFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value()), "2019-02-19 01:20:00");
    }
}

TEST_F(TimeFunctionTest, Test_subDateFunc) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));
        args.push_back(int64_t(20));

        SubDateFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value()), "2019-11-18 05:10:00");
    }
}

TEST_F(TimeFunctionTest, Test_subTimeFunc) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-2-18 05:10:00"));
        args.push_back(std::string("20:10:00"));

        SubTimeFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value()), "2019-02-17 09:00:00");
    }
}


TEST_F(TimeFunctionTest, Test_convertTz) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));
        args.push_back(std::string("-01:00"));
        args.push_back(std::string("+02:20"));

        ConvertTzFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value()), "2019-12-08 08:30:00");
    }
}

TEST_F(TimeFunctionTest, Test_currentDate) {
    // Test succeed
    {
        Timezone timezone;
        timezone.eastern = '+';
        timezone.hour = 8;
        timezone.minute = 20;

        CurrentDateFunc func;
        ASSERT_TRUE(func.initNebulaTime({}, &timezone).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
    }
}

TEST_F(TimeFunctionTest, Test_currentTime) {
    // Test succeed
    {
        Timezone timezone;
        timezone.eastern = '+';
        timezone.hour = 8;
        timezone.minute = 20;
        CurrentTimeFunc func;
        ASSERT_TRUE(func.initNebulaTime({}, &timezone).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
    }
}

TEST_F(TimeFunctionTest, Test_year) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));

        YearFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(2019, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_month) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));

        MonthFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(12, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_day) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));

        DayFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(8, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_dayOfMonth) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));

        DayOfMonthFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(8, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_dayOfWeek) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));

        DayOfWeekFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("Sun", boost::get<std::string>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_dayOfYear) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));

        DayOfYearFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(342, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_hour) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));

        HourFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(5, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_minute) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:00"));

        MinuteFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(10, boost::get<int64_t>(status.value()));
    }
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("20191208051000"));

        MinuteFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(10, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_second) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:30"));

        SecondFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(30, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_timeFormat) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:30"));
        args.push_back(std::string("%Y%m%d%H%M%S"));

        TimeFormatFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("20191208051030", boost::get<std::string>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_timeToSec) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019-12-08 05:10:30"));

        TimeToSecFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(18630, boost::get<int64_t>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_makeDate) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(std::string("2019"));
        args.push_back(int64_t(125));

        MakeDateFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("2019-04-05", boost::get<std::string>(status.value()));
    }

    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(int64_t(2019));
        args.push_back(int64_t(25));

        MakeDateFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("2019-01-25", boost::get<std::string>(status.value()));
    }
}

TEST_F(TimeFunctionTest, Test_makeTime) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(int64_t(12));
        args.push_back(int64_t(30));
        args.push_back(int64_t(45));

        MakeTimeFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("12:30:45", boost::get<std::string>(status.value()));
    }

    // Test failed
    {
        std::vector<VariantType> args;
        args.push_back(int64_t(12));
        args.push_back(int64_t(60));
        args.push_back(int64_t(45));

        MakeTimeFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_FALSE(status.ok());
    }
}

TEST_F(TimeFunctionTest, Test_utcDate) {
    // Test succeed
    {
        UTCDateFunc func;
        ASSERT_TRUE(func.initNebulaTime().ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
    }
}

TEST_F(TimeFunctionTest, Test_utcTime) {
    // Test succeed
    {
        UTCTimeFunc func;
        ASSERT_TRUE(func.initNebulaTime().ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
    }
}

TEST_F(TimeFunctionTest, Test_utcTimestamp) {
    // Test succeed
    {
        UTCTimestampFunc func;
        ASSERT_TRUE(func.initNebulaTime().ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
    }
}

TEST_F(TimeFunctionTest, Test_unixTimestamp) {
    // Test succeed
    {
        UnixTimestampFunc func;
        ASSERT_TRUE(func.initNebulaTime().ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
    }

    // Test succeed
    {
        std::vector<VariantType> args;
        args.emplace_back(std::string("1970-01-01 12:00:00"));
        UnixTimestampFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<int64_t>(status.value()), 43200);
    }
}

TEST_F(TimeFunctionTest, Test_timestampFormat) {
    // Test succeed
    {
        std::vector<VariantType> args;
        args.push_back(int64_t(90065));
        args.push_back(std::string("%Y%m%d%H%M%S"));

        TimestampFormatFunc func;
        ASSERT_TRUE(func.initNebulaTime(args).ok());
        auto status = func.getResult();
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("19700102010105", boost::get<std::string>(status.value()));
    }
}

}  // namespace nebula
