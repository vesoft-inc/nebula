/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <cstdint>

#include <folly/String.h>
#include <folly/hash/Hash.h>

#include "common/datatypes/Date.h"

namespace nebula {

const int64_t kDaysSoFar[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
const int64_t kLeapDaysSoFar[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

Date::Date(uint64_t days) {
    fromInt(days);
}

int64_t Date::toInt() const {
    // Year
    int64_t yearsPassed = year + 32768L;
    int64_t days = yearsPassed * 365L;
    // Add one day per leap year
    if (yearsPassed > 0) {
        days += (yearsPassed - 1) / 4 + 1;
    }

    // Month
    if (yearsPassed % 4 == 0) {
        // Leap year
        days += kLeapDaysSoFar[month - 1];
    } else {
        days += kDaysSoFar[month - 1];
    }

    // Day
    days += day;

    // Since we start from -32768/1/1, we need to reduce one day
    return days - 1;
}

void Date::fromInt(int64_t days) {
    // year
    int64_t yearsPassed = (days + 1) / 365;
    year = yearsPassed - 32768;
    int64_t daysInYear = (days + 1) % 365;

    // Deduce the number of days for leap years
    if (yearsPassed > 0) {
        daysInYear -= (yearsPassed - 1) / 4 + 1;
    }

    // Adjust the year if necessary
    while (daysInYear <= 0) {
        year = year - 1;
        if (year % 4 == 0) {
            // Leap year
            daysInYear += 366;
        } else {
            daysInYear += 365;
        }
    }

    // Month and day
    month = 1;
    while (true) {
        if (year % 4 == 0) {
            // Leap year
            if (daysInYear <= kLeapDaysSoFar[month]) {
                day = daysInYear - kLeapDaysSoFar[month - 1];
                break;
            }
        } else {
            if (daysInYear <= kDaysSoFar[month]) {
                day = daysInYear - kDaysSoFar[month - 1];
                break;
            }
        }
        month++;
    }
}

Date Date::operator+(int64_t days) const {
    int64_t daysSince = toInt();
    return Date(daysSince + days);
}

Date Date::operator-(int64_t days) const {
    int64_t daysSince = toInt();
    return Date(daysSince - days);
}

std::string Date::toString() const {
    // It's in current timezone already
    return folly::stringPrintf("%d-%02d-%02d", year, month, day);
}

std::string Time::toString() const {
    // It's in current timezone already
    return folly::stringPrintf("%02d:%02d:%02d.%06d", hour, minute, sec, microsec);
}

std::string DateTime::toString() const {
    // It's in current timezone already
    return folly::stringPrintf("%hd-%02hhu-%02hhu"
                               "T%02hhu:%02hhu:%02hhu.%u",
                               static_cast<int16_t>(year),
                               static_cast<uint8_t>(month),
                               static_cast<uint8_t>(day),
                               static_cast<uint8_t>(hour),
                               static_cast<uint8_t>(minute),
                               static_cast<uint8_t>(sec),
                               static_cast<uint32_t>(microsec));
}

}   // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Date>::operator()(const nebula::Date& h) const noexcept {
    size_t hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.year), sizeof(h.year));
    hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.month), sizeof(h.month), hv);
    return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.day), sizeof(h.day), hv);
}

std::size_t hash<nebula::Time>::operator()(const nebula::Time& h) const noexcept {
    std::size_t hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.hour), sizeof(h.hour));
    hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.minute), sizeof(h.minute), hv);
    hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.sec), sizeof(h.sec), hv);
    return folly::hash::fnv64_buf(
        reinterpret_cast<const void*>(&h.microsec), sizeof(h.microsec), hv);
}

std::size_t hash<nebula::DateTime>::operator()(const nebula::DateTime& h) const noexcept {
    return h.qword;
}

}   // namespace std
