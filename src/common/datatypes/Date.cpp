/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/datatypes/Date.h"

namespace nebula {

static const int64_t daysSoFar[] =
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
static const int64_t leapDaysSoFar[] =
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};


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
        days += leapDaysSoFar[month - 1];
    } else {
        days += daysSoFar[month - 1];
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
            if (daysInYear <= leapDaysSoFar[month]) {
                day = daysInYear - leapDaysSoFar[month - 1];
                break;
            }
        } else {
            if (daysInYear <= daysSoFar[month]) {
                day = daysInYear - daysSoFar[month - 1];
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
    // TODO(shylock) The format should depend on the locale
    return folly::stringPrintf("%d/%02d/%02d", year, month, day);
}

std::string Time::toString() const {
    // TODO(shylock) The format should depend on the locale
    return folly::stringPrintf("%02d:%02d:%02d.%06d",
                               hour,
                               minute,
                               sec,
                               microsec);
}


std::string DateTime::toString() const {
    // TODO(shylock) The format should depend on the locale
    return folly::stringPrintf("%d/%02d/%02d %02d:%02d:%02d.%06d",
                               year,
                               month,
                               day,
                               hour,
                               minute,
                               sec,
                               microsec);
}

}  // namespace nebula
