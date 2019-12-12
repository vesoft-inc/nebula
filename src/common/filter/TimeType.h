/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef COMMON_FILTER_TIMETYPE_H
#define COMMON_FILTER_TIMETYPE_H

#include "base/Base.h"

namespace nebula {
enum DateTimeType {
    ErrorType = -1,
    NullType = 0,
    DataType = 1,
    DataTimeType = 2,
    TimeType = 3,
    TimestampType = 4,
};

struct Interval {
    int16_t  year;
    int8_t   month;
    int8_t   day;
    int8_t   hour;
    int8_t   minute;
    int8_t   second;
    uint64_t secondPart;
};

enum IntervalType {
    MICROSECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    YEAR,
};

struct NebulaTime {
    NebulaTime() : year(0),
                   month(0),
                   day(0),
                   hour(0),
                   minute(0),
                   second(0),
                   secondPart(0),
                   timestamp(0),
                   timeType(NullType) {}
    uint32_t     year;
    uint32_t     month;
    uint32_t     day;
    uint32_t     hour;
    uint32_t     minute;
    uint32_t     second;
    uint32_t     secondPart; /**< microseconds */
    int64_t      timestamp;
    DateTimeType timeType;
};
}  // namespace nebula
#endif  //  COMMON_FILTER_TIMETYPE_H
