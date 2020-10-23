/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/function/TimeFunction.h"
#include "common/time/TimeUtils.h"

namespace nebula {

const uint8_t daysOfMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

// The mainstream Linux kernel's implementation constrains this
constexpr int64_t kMaxTimestamp = std::numeric_limits<int64_t>::max() / 1000000000;

static const std::regex datetimeReg("^([1-9]\\d{3})-"
                                    "(0[1-9]|1[0-2]|\\d)-"
                                    "(0[1-9]|[1-2][0-9]|3[0-1]|\\d)\\s+"
                                    "(20|21|22|23|[0-1]\\d|\\d):"
                                    "([0-5]\\d|\\d):"
                                    "([0-5]\\d|\\d)$");

StatusOr<Timestamp> TimeFunction::toTimestamp(const Value &val) {
    if (!val.isInt() && !val.isStr()) {
        return Status::Error("Incorrect timestamp type: `%s'", val.toString().c_str());
    }

    Timestamp timestamp;
    if (val.isStr()) {
        std::smatch result;
        if (!std::regex_match(val.getStr(), result, datetimeReg)) {
            return Status::Error("Incorrect timestamp type: `%s'", val.toString().c_str());
        }
        auto year = atoi(result[1].str().c_str());
        auto month = atoi(result[2].str().c_str());
        auto day = atoi(result[3].str().c_str());

        struct tm time;
        memset(&time, 0, sizeof(time));
        time.tm_year = year - 1900;
        time.tm_mon = month - 1;
        time.tm_mday = day;
        time.tm_hour = atoi(result[4].str().c_str());
        time.tm_min = atoi(result[5].str().c_str());
        time.tm_sec = atoi(result[6].str().c_str());

        // check day
        if (month != 2) {
            if (day > daysOfMonth[month - 1]) {
                return Status::Error("Incorrect timestamp type: `%s'", val.toString().c_str());
            }
        } else {
            if (time::TimeUtils::isLeapYear(year)) {
                if (day > 29) {
                    return Status::Error("Incorrect timestamp type: `%s'", val.toString().c_str());
                }
            } else if (day > 28) {
                return Status::Error("Incorrect timestamp type: `%s'", val.toString().c_str());
            }
        }

        timestamp = mktime(&time);
    } else {
        timestamp = val.getInt();
    }

    if (timestamp < 0 || (timestamp > kMaxTimestamp)) {
        return Status::Error("Incorrect timestamp type: `%s'", val.toString().c_str());
    }
    return timestamp;
}


StatusOr<Date> TimeFunction::toDate(const Value&) {
    LOG(FATAL) << "Unsupported";
    return Date();
}


StatusOr<DateTime> TimeFunction::toDateTime(const Value&) {
    LOG(FATAL) << "Unsupported";
    return DateTime();
}

}  // namespace nebula

