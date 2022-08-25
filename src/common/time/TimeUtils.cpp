/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/time/TimeUtils.h"

#include <limits>

#include "common/fs/FileUtils.h"
#include "common/time/TimezoneInfo.h"
#include "common/time/parser/DatetimeReader.h"

namespace nebula {
namespace time {

// The mainstream Linux kernel's implementation constrains this
constexpr int64_t kMaxTimestamp = std::numeric_limits<int64_t>::max() / 1000000000;

/*static*/ StatusOr<DateTime> TimeUtils::dateTimeFromMap(const Map &m) {
  // TODO(shylock) support timezone parameter
  DateTime dt;
  for (const auto &kv : m.kvs) {
    if (!kv.second.isInt()) {
      return Status::Error("Invalid value type.");
    }
    if (kv.first == "year") {
      auto result = validateYear(kv.second.getInt());
      if (!result.ok()) {
        return result;
      }

      dt.year = kv.second.getInt();
    } else if (kv.first == "month") {
      if (kv.second.getInt() <= 0 || kv.second.getInt() > 12) {
        return Status::Error("Invalid month number `%ld'.", kv.second.getInt());
      }
      dt.month = kv.second.getInt();
    } else if (kv.first == "day") {
      if (kv.second.getInt() <= 0 || kv.second.getInt() > 31) {
        return Status::Error("Invalid day number `%ld'.", kv.second.getInt());
      }
      dt.day = kv.second.getInt();
    } else if (kv.first == "hour") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 23) {
        return Status::Error("Invalid hour number `%ld'.", kv.second.getInt());
      }
      dt.hour = kv.second.getInt();
    } else if (kv.first == "minute") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 59) {
        return Status::Error("Invalid minute number `%ld'.", kv.second.getInt());
      }
      dt.minute = kv.second.getInt();
    } else if (kv.first == "second") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 59) {
        return Status::Error("Invalid second number `%ld'.", kv.second.getInt());
      }
      dt.sec = kv.second.getInt();
    } else if (kv.first == "millisecond") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 999) {
        return Status::Error("Invalid millisecond number `%ld'.", kv.second.getInt());
      }
      dt.microsec += kv.second.getInt() * 1000;
    } else if (kv.first == "microsecond") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 999) {
        return Status::Error("Invalid microsecond number `%ld'.", kv.second.getInt());
      }
      dt.microsec += kv.second.getInt();
    } else {
      return Status::Error("Invalid parameter `%s'.", kv.first.c_str());
    }
  }
  auto result = validateDate(dt);
  if (!result.ok()) {
    return result;
  }
  return dt;
}

/*static*/ StatusOr<Date> TimeUtils::dateFromMap(const Map &m) {
  Date d;
  for (const auto &kv : m.kvs) {
    if (!kv.second.isInt()) {
      return Status::Error("Invalid value type.");
    }
    if (kv.first == "year") {
      auto result = validateYear(kv.second.getInt());
      if (!result.ok()) {
        return result;
      }
      d.year = kv.second.getInt();
    } else if (kv.first == "month") {
      if (kv.second.getInt() <= 0 || kv.second.getInt() > 12) {
        return Status::Error("Invalid month number `%ld'.", kv.second.getInt());
      }
      d.month = kv.second.getInt();
    } else if (kv.first == "day") {
      if (kv.second.getInt() <= 0 || kv.second.getInt() > 31) {
        return Status::Error("Invalid day number `%ld'.", kv.second.getInt());
      }
      d.day = kv.second.getInt();
    } else {
      return Status::Error("Invalid parameter `%s'.", kv.first.c_str());
    }
  }
  auto result = validateDate(d);
  if (!result.ok()) {
    return result;
  }
  return d;
}

/*static*/ StatusOr<Time> TimeUtils::timeFromMap(const Map &m) {
  Time t;
  for (const auto &kv : m.kvs) {
    if (!kv.second.isInt()) {
      return Status::Error("Invalid value type.");
    }
    if (kv.first == "hour") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 23) {
        return Status::Error("Invalid hour number `%ld'.", kv.second.getInt());
      }
      t.hour = kv.second.getInt();
    } else if (kv.first == "minute") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 59) {
        return Status::Error("Invalid minute number `%ld'.", kv.second.getInt());
      }
      t.minute = kv.second.getInt();
    } else if (kv.first == "second") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 59) {
        return Status::Error("Invalid second number `%ld'.", kv.second.getInt());
      }
      t.sec = kv.second.getInt();
    } else if (kv.first == "millisecond") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 999) {
        return Status::Error("Invalid millisecond number `%ld'.", kv.second.getInt());
      }
      t.microsec += kv.second.getInt() * 1000;
    } else if (kv.first == "microsecond") {
      if (kv.second.getInt() < 0 || kv.second.getInt() > 999) {
        return Status::Error("Invalid microsecond number `%ld'.", kv.second.getInt());
      }
      t.microsec += kv.second.getInt();
    } else {
      return Status::Error("Invalid parameter `%s'.", kv.first.c_str());
    }
  }
  return t;
}

StatusOr<Value> TimeUtils::toTimestamp(const Value &val) {
  Timestamp timestamp;
  if (val.isStr()) {
    auto status = parseDateTime(val.getStr());
    if (!status.ok()) {
      return status.status();
    }
    auto result = std::move(status).value();
    auto dateTime = result.withTimeZone ? result.dt : dateTimeToUTC(result.dt);
    if (dateTime.microsec != 0) {
      return Status::Error("The timestamp  only supports seconds unit.");
    }
    timestamp = time::TimeConversion::dateTimeToUnixSeconds(dateTime);
  } else if (val.isInt()) {
    timestamp = val.getInt();
  } else if (val.isDateTime()) {
    timestamp = time::TimeConversion::dateTimeToUnixSeconds(val.getDateTime());
  } else {
    return Status::Error("Incorrect timestamp type: `%s'", val.toString().c_str());
  }

  if (timestamp < 0 || (timestamp > kMaxTimestamp)) {
    return Status::Error("Incorrect timestamp value: `%s'", val.toString().c_str());
  }
  return timestamp;
}

/*static*/ StatusOr<Duration> TimeUtils::durationFromMap(const Map &m) {
  Duration d;
  for (const auto &kv : m.kvs) {
    if (!kv.second.isInt()) {
      return Status::Error("Invalid value type.");
    }
    if (kv.first == "years") {
      d.addYears(kv.second.getInt());
    } else if (kv.first == "quarters") {
      d.addQuarters(kv.second.getInt());
    } else if (kv.first == "months") {
      d.addMonths(kv.second.getInt());
    } else if (kv.first == "weeks") {
      d.addWeeks(kv.second.getInt());
    } else if (kv.first == "days") {
      d.addDays(kv.second.getInt());
    } else if (kv.first == "hours") {
      d.addHours(kv.second.getInt());
    } else if (kv.first == "minutes") {
      d.addMinutes(kv.second.getInt());
    } else if (kv.first == "seconds") {
      d.addSeconds(kv.second.getInt());
    } else if (kv.first == "milliseconds") {
      d.addMilliseconds(kv.second.getInt());
    } else if (kv.first == "microseconds") {
      d.addMicroseconds(kv.second.getInt());
    } else {
      return Status::Error("Unkown field %s.", kv.first.c_str());
    }
  }
  return d;
}

/*static*/ StatusOr<Result> TimeUtils::parseDateTime(const std::string &str) {
  auto p = DatetimeReader();
  auto result = p.readDatetime(str);
  NG_RETURN_IF_ERROR(result);
  return result.value();
}

/*static*/ StatusOr<Date> TimeUtils::parseDate(const std::string &str) {
  auto p = DatetimeReader();
  auto result = p.readDate(str);
  NG_RETURN_IF_ERROR(result);
  return result.value();
}

/*static*/ StatusOr<TimeResult> TimeUtils::parseTime(const std::string &str) {
  auto p = DatetimeReader();
  return p.readTime(str);
}

}  // namespace time
}  // namespace nebula
