/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_DATE_H_
#define DATATYPES_DATE_H_

#include "base/Base.h"

namespace nebula {

struct Date {
    int16_t year;
    int8_t month;
    int8_t day;

    bool operator==(const Date& rhs) const {
        return year == rhs.year &&
               month == rhs.month &&
               day == rhs.day;
    }
};


struct DateTime {
    int16_t year;
    int8_t month;
    int8_t day;
    int8_t hour;
    int8_t minute;
    int8_t sec;
    int32_t microsec;
    int32_t timezone;

    bool operator==(const DateTime& rhs) const {
        return year == rhs.year &&
               month == rhs.month &&
               day == rhs.day &&
               hour == rhs.hour &&
               minute == rhs.minute &&
               sec == rhs.sec &&
               microsec == rhs.microsec &&
               timezone == rhs.timezone;
    }
};

}  // namespace nebula
#endif  // DATATYPES_DATE_H_
