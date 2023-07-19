/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_DURATION_H
#define COMMON_DATATYPES_DURATION_H

#include <folly/dynamic.h>

#include <sstream>

#include "common/time/Constants.h"

namespace nebula {

// Duration equals to months + seconds + microseconds
// The base between months and days is not fixed, so we store years and months
// separately.
struct Duration {
  // day + hours + minutes + seconds + microseconds
  int64_t seconds;
  int32_t microseconds;
  // years + months
  int32_t months;

  Duration() : seconds(0), microseconds(0), months(0) {}
  Duration(int32_t m, int64_t s, int32_t us) : seconds(s), microseconds(us), months(m) {}

  int64_t years() const {
    return months / 12;
  }

  int64_t monthsInYear() const {
    return months % 12;
  }

  int64_t days() const {
    return seconds / time::kSecondsOfDay;
  }

  int64_t hours() const {
    return seconds % time::kSecondsOfDay / time::kSecondsOfHour;
  }

  int64_t minutes() const {
    return seconds % time::kSecondsOfHour / time::kSecondsOfMinute;
  }

  int64_t secondsInMinute() const {
    return seconds % time::kSecondsOfMinute;
  }

  int64_t microsecondsInSecond() const {
    return microseconds;
  }

  Duration operator-() const {
    return Duration(-months, -seconds, -microseconds);
  }

  Duration operator+(const Duration& rhs) const {
    return Duration(months + rhs.months, seconds + rhs.seconds, microseconds + rhs.microseconds);
  }

  Duration operator-(const Duration& rhs) const {
    return Duration(months - rhs.months, seconds - rhs.seconds, microseconds - rhs.microseconds);
  }

  Duration& addYears(int32_t y) {
    months += y * 12;
    return *this;
  }

  Duration& addQuarters(int32_t q) {
    months += q * 3;
    return *this;
  }

  Duration& addMonths(int32_t m) {
    months += m;
    return *this;
  }

  Duration& addWeeks(int32_t w) {
    seconds += (w * 7 * time::kSecondsOfDay);
    return *this;
  }

  Duration& addDays(int64_t d) {
    seconds += d * time::kSecondsOfDay;
    return *this;
  }

  Duration& addHours(int64_t h) {
    seconds += h * time::kSecondsOfHour;
    return *this;
  }

  Duration& addMinutes(int64_t minutes) {
    seconds += minutes * time::kSecondsOfMinute;
    return *this;
  }

  Duration& addSeconds(int64_t s) {
    seconds += s;
    return *this;
  }

  Duration& addMilliseconds(int64_t ms) {
    seconds += ms / 1000;
    microseconds += ((ms % 1000) * 1000);
    return *this;
  }

  Duration& addMicroseconds(int32_t us) {
    microseconds += us;
    return *this;
  }

  // can't compare
  bool operator<(const Duration& rhs) const = delete;

  bool operator==(const Duration& rhs) const {
    return months == rhs.months && seconds == rhs.seconds && microseconds == rhs.microseconds;
  }

  std::string toString() const {
    return folly::sformat(
        "P{}MT{}.{:0>6}000S", months, seconds + microseconds / 1000000, microseconds % 1000000);
  }

  folly::dynamic toJson() const {
    return toString();
  }
};

}  // namespace nebula

namespace std {

// Inject a customized hash function
template <>
struct hash<nebula::Duration> {
  std::size_t operator()(const nebula::Duration& d) const noexcept {
    size_t hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&d.months), sizeof(d.months));
    hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(d.seconds), sizeof(d.seconds), hv);
    return folly::hash::fnv64_buf(
        reinterpret_cast<const void*>(d.microseconds), sizeof(d.microseconds), hv);
  }
};

}  // namespace std
#endif
