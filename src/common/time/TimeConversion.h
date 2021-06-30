/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_TIME_TIMECONVERSION_H_
#define COMMON_TIME_TIMECONVERSION_H_

#include <cstdint>

#include "common/datatypes/Date.h"

namespace nebula {
namespace time {

class TimeConversion {
public:
    explicit TimeConversion(...) = delete;

    static int64_t dateTimeDiffSeconds(const DateTime &dateTime0, const DateTime &dateTime1);

    // unix time
    static int64_t dateTimeToUnixSeconds(const DateTime &dateTime) {
        return dateTimeDiffSeconds(dateTime, kEpoch);
    }

    static DateTime unixSecondsToDateTime(int64_t seconds);

    // Shift the DateTime in timezone space
    static DateTime dateTimeShift(const DateTime &dateTime, int64_t offsetSeconds) {
        if (offsetSeconds == 0) {
            return dateTime;
        }
        auto dt = unixSecondsToDateTime(dateTimeToUnixSeconds(dateTime) + offsetSeconds);
        dt.microsec = dateTime.microsec;
        return dt;
    }

    // unix time
    static int64_t dateToUnixSeconds(const Date &date) {
        return dateTimeDiffSeconds(DateTime(date), kEpoch);
    }

    static Date unixSecondsToDate(int64_t seconds) {
        auto dateTime = unixSecondsToDateTime(seconds);
        return Date(dateTime.year, dateTime.month, dateTime.day);
    }

    // Shift the DateTime in timezone space
    static Date dateShift(const Date &date, int64_t offsetSeconds) {
        if (offsetSeconds == 0) {
            return date;
        }
        return unixSecondsToDate(dateToUnixSeconds(date) + offsetSeconds);
    }

    // unix time
    static int64_t timeToSeconds(const Time &time) {
        int64_t seconds = time.sec;
        seconds += (time.minute * kSecondsOfMinute);
        seconds += (time.hour * kSecondsOfHour);
        return seconds;
    }

    static Time unixSecondsToTime(int64_t seconds) {
        Time t;
        auto dt = unixSecondsToDateTime(seconds);
        t.hour = dt.hour;
        t.minute = dt.minute;
        t.sec = dt.sec;
        return t;
    }

    // Shift the Time in timezone space
    static Time timeShift(const Time &time, int64_t offsetSeconds) {
        if (offsetSeconds == 0) {
            return time;
        }
        auto t = unixSecondsToTime(timeToSeconds(time) + offsetSeconds);
        t.microsec = time.microsec;
        return t;
    }

    // https://en.wikipedia.org/wiki/Leap_year#Leap_day
    static bool isLeapYear(int16_t year) {
        if (year % 4 != 0) {
            return false;
        } else if (year % 100 != 0) {
            return true;
        } else if (year % 400 != 0) {
            return false;
        } else {
            return true;
        }
    }

    static const DateTime kEpoch;

    static constexpr int kDayOfLeapYear = 366;
    static constexpr int kDayOfCommonYear = 365;

    static constexpr int64_t kSecondsOfMinute = 60;
    static constexpr int64_t kSecondsOfHour = 60 * kSecondsOfMinute;
    static constexpr int64_t kSecondsOfDay = 24 * kSecondsOfHour;

private:
    // The result of a right-shift of a signed negative number is implementation-dependent
    // (UB. see https://en.cppreference.com/w/cpp/language/operator_arithmetic).
    // So make sure the result is what we expected, if right shift not filled highest bit by the
    // sign bit that the process will falls back to procedure which fill hightest bit by the sign
    // bit value.
    static int64_t shr(int64_t a, int b) {
        int64_t one = 1;
        return (-one >> 1 == -1 ? a >> b : (a + (a < 0)) / (one << b) - (a < 0));
    }
};

}  // namespace time
}  // namespace nebula

#endif  // COMMON_TIME_TIMECONVERSION_H_
