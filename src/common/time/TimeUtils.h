/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_TIME_TIME_H_
#define COMMON_TIME_TIME_H_

#include <boost/algorithm/string/case_conv.hpp>
#include <iomanip>
#include <limits>
#include <sstream>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Date.h"
#include "common/datatypes/Duration.h"
#include "common/datatypes/Map.h"
#include "common/fs/FileUtils.h"
#include "common/time/TimeConversion.h"
#include "common/time/TimezoneInfo.h"
#include "common/time/WallClock.h"
#include "common/time/parser/Result.h"

namespace nebula {
namespace time {

// In nebula only store UTC time, and the interpretation of time value based on
// the timezone configuration in current system.

class TimeUtils {
 public:
  explicit TimeUtils(...) = delete;

  // check the validation of date
  // not check range limit in here
  // I.E. 2019-02-31
  template <
      typename D,
      typename = std::enable_if_t<std::is_same<D, Date>::value || std::is_same<D, DateTime>::value>>
  static Status validateDate(const D &date) {
    // first check month
    if (date.month < 1 || date.month > 12) {
      return Status::Error("Invalid month number.`%s' is not a valid date.",
                           date.toString().c_str());
    }
    const int64_t *p = isLeapYear(date.year) ? kLeapDaysSoFar : kDaysSoFar;
    if ((p[date.month] - p[date.month - 1]) < date.day) {
      return Status::Error("`%s' is not a valid date.", date.toString().c_str());
    }
    return Status::OK();
  }

  static Status validateYear(int64_t year) {
    if (year < std::numeric_limits<int16_t>::min() || year > std::numeric_limits<int16_t>::max()) {
      return Status::Error("Out of range year `%ld'.", year);
    }
    return Status::OK();
  }

  template <
      typename D,
      typename = std::enable_if_t<std::is_same<D, Time>::value || std::is_same<D, DateTime>::value>>
  static Status validateTime(const D &time) {
    if (time.hour < 0 || time.hour >= 24) {
      return Status::Error("Invalid hour number %d.", time.hour);
    }
    if (time.minute < 0 || time.minute >= 60) {
      return Status::Error("Invalid minute number %d.", time.minute);
    }
    if (time.sec < 0 || time.sec >= 60) {
      return Status::Error("Invalid second number %d.", time.sec);
    }
    if (time.microsec < 0 || time.microsec >= 1000000) {
      return Status::Error("Invalid microsecond number %d.", time.microsec);
    }
    return Status::OK();
  }

  static StatusOr<Result> parseDateTime(const std::string &str);

  static StatusOr<DateTime> dateTimeFromMap(const Map &m);

  // utc + offset = local
  static DateTime dateTimeToUTC(const DateTime &dateTime) {
    return TimeConversion::dateTimeShift(dateTime, -Timezone::getGlobalTimezone().utcOffsetSecs());
  }

  static DateTime utcToDateTime(const DateTime &dateTime) {
    return TimeConversion::dateTimeShift(dateTime, Timezone::getGlobalTimezone().utcOffsetSecs());
  }

  static DateTime localDateTime() {
    auto time = unixTime();
    auto dt = TimeConversion::unixSecondsToDateTime(time.seconds -
                                                    Timezone::getGlobalTimezone().utcOffsetSecs());
    dt.microsec = time.milliseconds * 1000;
    return dt;
  }

  static DateTime utcDateTime() {
    auto time = unixTime();
    auto dt = TimeConversion::unixSecondsToDateTime(time.seconds);
    dt.microsec = time.milliseconds * 1000;
    return dt;
  }

  static Value getDateTimeAttr(const DateTime &dt, const std::string &prop) {
    auto lowerProp = boost::algorithm::to_lower_copy(prop);
    if (lowerProp == "year") {
      return static_cast<int>(dt.year);
    } else if (lowerProp == "month") {
      return static_cast<int>(dt.month);
    } else if (lowerProp == "day") {
      return static_cast<int>(dt.day);
    } else if (lowerProp == "hour") {
      return static_cast<int>(dt.hour);
    } else if (lowerProp == "minute") {
      return static_cast<int>(dt.minute);
    } else if (lowerProp == "second") {
      return static_cast<int>(dt.sec);
    } else if (lowerProp == "microsecond") {
      return static_cast<int>(dt.microsec);
    } else {
      return Value::kNullUnknownProp;
    }
  }

  static StatusOr<Date> dateFromMap(const Map &m);

  static StatusOr<Date> parseDate(const std::string &str);

  static StatusOr<Date> localDate() {
    Date d;
    time_t unixTime = std::time(NULL);
    if (unixTime == -1) {
      return Status::Error("Get unix time failed: %s.", std::strerror(errno));
    }
    return TimeConversion::unixSecondsToDate(unixTime -
                                             Timezone::getGlobalTimezone().utcOffsetSecs());
  }

  static StatusOr<Date> utcDate() {
    Date d;
    time_t unixTime = std::time(NULL);
    if (unixTime == -1) {
      return Status::Error("Get unix time failed: %s.", std::strerror(errno));
    }
    return TimeConversion::unixSecondsToDate(unixTime);
  }

  static Value getDateAttr(const Date &d, const std::string &prop) {
    auto lowerProp = boost::algorithm::to_lower_copy(prop);
    if (lowerProp == "year") {
      return d.year;
    } else if (lowerProp == "month") {
      return d.month;
    } else if (lowerProp == "day") {
      return d.day;
    } else {
      return Value::kNullUnknownProp;
    }
  }

  static StatusOr<Time> timeFromMap(const Map &m);

  static StatusOr<TimeResult> parseTime(const std::string &str);

  // utc + offset = local
  static Time timeToUTC(const Time &time) {
    return TimeConversion::timeShift(time, -Timezone::getGlobalTimezone().utcOffsetSecs());
  }

  static Time utcToTime(const Time &time) {
    return TimeConversion::timeShift(time, Timezone::getGlobalTimezone().utcOffsetSecs());
  }

  static Time localTime() {
    auto time = unixTime();
    auto t = TimeConversion::unixSecondsToTime(time.seconds -
                                               Timezone::getGlobalTimezone().utcOffsetSecs());
    t.microsec = time.milliseconds * 1000;
    return t;
  }

  static Time utcTime() {
    auto time = unixTime();
    auto t = TimeConversion::unixSecondsToTime(time.seconds);
    t.microsec = time.milliseconds * 1000;
    return t;
  }

  static Value getTimeAttr(const Time &t, const std::string &prop) {
    auto lowerProp = boost::algorithm::to_lower_copy(prop);
    if (lowerProp == "hour") {
      return t.hour;
    } else if (lowerProp == "minute") {
      return t.minute;
    } else if (lowerProp == "second") {
      return t.sec;
    } else if (lowerProp == "microsecond") {
      return t.microsec;
    } else {
      return Value::kNullUnknownProp;
    }
  }

  static StatusOr<Value> toTimestamp(const Value &val);

  static StatusOr<Duration> durationFromMap(const Map &m);

 private:
  struct UnixTime {
    int64_t seconds{0};
    int64_t milliseconds{0};
  };

  // <seconds, milliseconds>
  static UnixTime unixTime() {
    ::timespec ts;
    ::clock_gettime(CLOCK_REALTIME, &ts);
    return UnixTime{ts.tv_sec, ::lround(ts.tv_nsec / 1000000)};
  }
};  // class TimeUtils

}  // namespace time
}  // namespace nebula

#endif  // COMMON_TIME_TIME_H_
