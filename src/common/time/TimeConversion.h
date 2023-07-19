/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_TIME_TIMECONVERSION_H_
#define COMMON_TIME_TIMECONVERSION_H_

#include <cstdint>

#include "common/datatypes/Date.h"
#include "common/time/Constants.h"

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

  static const DateTime kEpoch;

 private:
  // The result of a right-shift of a signed negative number is
  // implementation-dependent (UB. see
  // https://en.cppreference.com/w/cpp/language/operator_arithmetic). So make
  // sure the result is what we expected, if right shift not filled highest bit
  // by the sign bit that the process will falls back to procedure which fill
  // highest bit by the sign bit value.
  static int64_t shr(int64_t a, int b) {
    int64_t one = 1;
    return (-one >> 1 == -1 ? a >> b : (a + (a < 0)) / (one << b) - (a < 0));
  }
};

}  // namespace time
}  // namespace nebula

#endif  // COMMON_TIME_TIMECONVERSION_H_
