/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <limits>

#include "common/fs/FileUtils.h"
#include "common/time/TimezoneInfo.h"
#include "common/time/TimeUtils.h"

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
            if (kv.second.getInt() < std::numeric_limits<int16_t>::min() ||
                kv.second.getInt() > std::numeric_limits<int16_t>::max()) {
                return Status::Error("Out of range year `%ld'.", kv.second.getInt());
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
        } else if (kv.first == "microsecond") {
            if (kv.second.getInt() < 0 || kv.second.getInt() > 999999) {
                return Status::Error("Invalid microsecond number `%ld'.", kv.second.getInt());
            }
            dt.microsec = kv.second.getInt();
        } else {
            return Status::Error("Invlaid parameter `%s'.", kv.first.c_str());
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
            if (kv.second.getInt() < std::numeric_limits<int16_t>::min() ||
                kv.second.getInt() > std::numeric_limits<int16_t>::max()) {
                return Status::Error("Out of range year `%ld'.", kv.second.getInt());
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
            return Status::Error("Invlaid parameter `%s'.", kv.first.c_str());
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
        } else if (kv.first == "microsecond") {
            if (kv.second.getInt() < 0 || kv.second.getInt() > 999999) {
                return Status::Error("Invalid microsecond number `%ld'.", kv.second.getInt());
            }
            t.microsec = kv.second.getInt();
        } else {
            return Status::Error("Invlaid parameter `%s'.", kv.first.c_str());
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
        auto dateTime = std::move(status).value();
        if (dateTime.microsec != 0) {
            return Status::Error("The timestamp  only supports seconds unit.");
        }
        timestamp = time::TimeConversion::dateTimeToUnixSeconds(dateTime);
    } else if (val.isInt()) {
        timestamp = val.getInt();
    } else {
        return Status::Error("Incorrect timestamp type: `%s'", val.toString().c_str());
    }

    if (timestamp < 0 || (timestamp > kMaxTimestamp)) {
        return Status::Error("Incorrect timestamp value: `%s'", val.toString().c_str());
    }
    return timestamp;
}

}   // namespace time
}   // namespace nebula
