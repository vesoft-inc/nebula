/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_TIME_TIME_H_
#define COMMON_TIME_TIME_H_

#include <iomanip>
#include <limits>
#include <vector>

#include <gflags/gflags_declare.h>

#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Date.h"
#include "common/datatypes/Map.h"
#include "common/fs/FileUtils.h"
#include "common/time/TimezoneInfo.h"

DECLARE_string(timezone_name);

namespace nebula {
namespace time {

// In nebula only store UTC time, and the interpretion of time value based on the
// timezone configuration in current system.

class TimeUtils {
public:
    explicit TimeUtils(...) = delete;

    // TODO(shylock) Get Timzone info(I.E. GMT offset) directly from IANA tzdb
    // to support non-global timezone configuration
    // See the timezone format from https://man7.org/linux/man-pages/man3/tzset.3.html
    static Status initializeGlobalTimezone();

    // check the validation of date
    // not check range limit in here
    // I.E. 2019-02-31
    template <typename D,
              typename = std::enable_if_t<std::is_same<D, Date>::value ||
                                          std::is_same<D, DateTime>::value>>
    static Status validateDate(const D &date) {
        const int64_t *p = isLeapYear(date.year) ? kLeapDaysSoFar : kDaysSoFar;
        if ((p[date.month] - p[date.month - 1]) < date.day) {
            return Status::Error("`%s' is not a valid date.", date.toString().c_str());
        }
        return Status::OK();
    }

    // TODO(shylock) support more format
    static StatusOr<DateTime> parseDateTime(const std::string &str) {
        std::tm tm;
        std::istringstream ss(str);
        ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
        if (ss.fail()) {
            return Status::Error();
        }
        DateTime dt;
        dt.year = tm.tm_year + 1900;
        dt.month = tm.tm_mon + 1;
        dt.day = tm.tm_mday;
        dt.hour = tm.tm_hour;
        dt.minute = tm.tm_min;
        dt.sec = tm.tm_sec;
        dt.microsec = 0;
        NG_RETURN_IF_ERROR(validateDate(dt));
        return dt;
    }

    static StatusOr<DateTime> dateTimeFromMap(const Map &m);

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
        return unixSecondsToDateTime(dateTimeToUnixSeconds(dateTime) + offsetSeconds);
    }

    static DateTime dateTimeToUTC(const DateTime &dateTime) {
        return dateTimeShift(dateTime, getGlobalTimezone().utcOffsetSecs());
    }

    static DateTime utcToDateTime(const DateTime &dateTime) {
        return dateTimeShift(dateTime, -getGlobalTimezone().utcOffsetSecs());
    }

    static StatusOr<DateTime> localDateTime() {
        DateTime dt;
        time_t unixTime = std::time(NULL);
        if (unixTime == -1) {
            return Status::Error("Get unix time failed: %s.", std::strerror(errno));
        }
        return unixSecondsToDateTime(unixTime - getGlobalTimezone().utcOffsetSecs());
    }

    static StatusOr<DateTime> utcDateTime() {
        DateTime dt;
        time_t unixTime = std::time(NULL);
        if (unixTime == -1) {
            return Status::Error("Get unix time failed: %s.", std::strerror(errno));
        }
        return unixSecondsToDateTime(unixTime);
    }

    static StatusOr<Date> dateFromMap(const Map &m);

    // TODO(shylock) support more format
    static StatusOr<Date> parseDate(const std::string &str) {
        std::tm tm;
        std::istringstream ss(str);
        ss >> std::get_time(&tm, "%Y-%m-%d");
        if (ss.fail()) {
            return Status::Error();
        }
        Date d;
        d.year = tm.tm_year + 1900;
        d.month = tm.tm_mon + 1;
        d.day = tm.tm_mday;
        NG_RETURN_IF_ERROR(validateDate(d));
        return d;
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

    static Date dateToUTC(const Date &date) {
        return dateShift(date, getGlobalTimezone().utcOffsetSecs());
    }

    static Date utcToDate(const Date &date) {
        return dateShift(date, -getGlobalTimezone().utcOffsetSecs());
    }

    static StatusOr<Date> localDate() {
        Date d;
        time_t unixTime = std::time(NULL);
        if (unixTime == -1) {
            return Status::Error("Get unix time failed: %s.", std::strerror(errno));
        }
        return unixSecondsToDate(unixTime - getGlobalTimezone().utcOffsetSecs());
    }

    static StatusOr<Date> utcDate() {
        Date d;
        time_t unixTime = std::time(NULL);
        if (unixTime == -1) {
            return Status::Error("Get unix time failed: %s.", std::strerror(errno));
        }
        return unixSecondsToDate(unixTime);
    }

    static StatusOr<Time> timeFromMap(const Map &m);

    // TODO(shylock) support more format
    static StatusOr<Time> parseTime(const std::string &str) {
        std::tm tm;
        std::istringstream ss(str);
        ss >> std::get_time(&tm, "%H:%M:%S");
        if (ss.fail()) {
            return Status::Error();
        }
        Time t;
        t.hour = tm.tm_hour;
        t.minute = tm.tm_min;
        t.sec = tm.tm_sec;
        t.microsec = 0;
        return t;
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
        return unixSecondsToTime(timeToSeconds(time) + offsetSeconds);
    }

    static Time timeToUTC(const Time &time) {
        return timeShift(time, getGlobalTimezone().utcOffsetSecs());
    }

    static Time utcToTime(const Time &time) {
        return timeShift(time, getGlobalTimezone().utcOffsetSecs());
    }

    static StatusOr<Time> localTime() {
        Time dt;
        time_t unixTime = std::time(NULL);
        if (unixTime == -1) {
            return Status::Error("Get unix time failed: %s.", std::strerror(errno));
        }
        return unixSecondsToTime(unixTime - getGlobalTimezone().utcOffsetSecs());
    }

    static StatusOr<Time> utcTime() {
        Time dt;
        time_t unixTime = std::time(NULL);
        if (unixTime == -1) {
            return Status::Error("Get unix time failed: %s.", std::strerror(errno));
        }
        return unixSecondsToTime(unixTime);
    }

    static Timezone &getGlobalTimezone() {
        return globalTimezone;
    }

    static StatusOr<Value> toTimestamp(const Value &val);

private:
    static constexpr int kDayOfLeapYear = 366;
    static constexpr int kDayOfCommonYear = 365;

    static constexpr int64_t kSecondsOfMinute = 60;
    static constexpr int64_t kSecondsOfHour = 60 * kSecondsOfMinute;
    static constexpr int64_t kSecondsOfDay = 24 * kSecondsOfHour;

    static const DateTime kEpoch;

    static Timezone globalTimezone;

    // The result of a right-shift of a signed negative number is implementation-dependent
    // (UB. see https://en.cppreference.com/w/cpp/language/operator_arithmetic).
    // So make sure the result is what we expected, if right shift not filled highest bit by the
    // sign bit that the process will falls back to procedure which fill hightest bit by the sign
    // bit value.
    static int64_t shr(int64_t a, int b) {
        int64_t one = 1;
        return (-one >> 1 == -1 ? a >> b : (a + (a < 0)) / (one << b) - (a < 0));
    }
};   // class TimeUtils

}   // namespace time
}   // namespace nebula

#endif   // COMMON_TIME_TIME_H_
