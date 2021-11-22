/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <unordered_map>

#include "common/time/TimeConversion.h"

namespace nebula {

// Duration equals to months + seconds + microseconds
// The base between months and days is not fixed, so we store years and months
// separately.
struct Duration {
  // years + months
  int64_t months;
  // day + hours + minutes + seconds + microseconds
  int64_t seconds;
  int64_t microseconds;

  int64_t years() const { return months / 12; }

  int64_t monthsInYear() const { return months % 12; }

  int64_t days() const { return seconds / time::TimeConversion::kSecondsOfDay; }

  int64_t hours() const {
    return seconds % time::TimeConversion::kSecondsOfDay / time::TimeConversion::kSecondsOfHour;
  }

  int64_t minutes() const {
    return seconds % time::TimeConversion::kSecondsOfHour / time::TimeConversion::kSecondsOfMinute;
  }

  int64_t secondsInMinute() const { return seconds % time::TimeConversion::kSecondsOfMinute; }

  int64_t microsecondsInSecond() const { return microseconds; }

  std::string toString() const {
    std::stringstream ss;
    ss << "P" << months << "M" << days() << "D"
       << "T" << seconds << "S";
    return ss.str();
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
