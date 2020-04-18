/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_DATE_H_
#define DATATYPES_DATE_H_

#include "base/Base.h"
#include <folly/hash/Hash.h>
#include <gtest/gtest_prod.h>

namespace nebula {

struct Date {
    FRIEND_TEST(Date, DaysConversion);

    int16_t year;   // Any integer
    int8_t month;   // 1 - 12
    int8_t day;     // 1 - 31

    Date() : year{0}, month{1}, day{1} {}
    Date(int16_t y, int8_t m, int8_t d) : year{y}, month{m}, day{d} {}
    // Tak the number of days since -32768/1/1, and convert to the real date
    explicit Date(uint64_t days);

    void clear() {
        year = 0;
        month = 1;
        day = 1;
    }

    void reset(int16_t y, int8_t m, int8_t d) {
        year = y;
        month = m;
        day = d;
    }

    bool operator==(const Date& rhs) const {
        return year == rhs.year &&
               month == rhs.month &&
               day == rhs.day;
    }

    Date operator+(int64_t days) const;
    Date operator-(int64_t days) const;

    std::string toString() const;

    // Return the number of days since -32768/1/1
    int64_t toInt() const;
    // Convert the number of days since -32768/1/1 to the real date
    void fromInt(int64_t days);
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

    void clear() {
        year = 0;
        month = 0;
        day = 0;
        hour = 0;
        minute = 0;
        sec = 0;
        microsec = 0;
        timezone = 0;
    }

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

    std::string toString() const;
};

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::Date> {
    std::size_t operator()(const nebula::Date& h) const noexcept {
        size_t hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.year),
                                           sizeof(h.year));
        hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.month),
                                    sizeof(h.month),
                                    hv);
        return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.day),
                                      sizeof(h.day),
                                      hv);
    }
};


template<>
struct hash<nebula::DateTime> {
    std::size_t operator()(const nebula::DateTime& h) const noexcept {
        size_t hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.year),
                                           sizeof(h.year));
        hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.month),
                                    sizeof(h.month),
                                    hv);
        hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.day),
                                    sizeof(h.day),
                                    hv);
        hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.hour),
                                    sizeof(h.hour),
                                    hv);
        hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.minute),
                                    sizeof(h.minute),
                                    hv);
        hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.sec),
                                    sizeof(h.sec),
                                    hv);
        hv = folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.microsec),
                                    sizeof(h.microsec),
                                    hv);
        return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.timezone),
                                      sizeof(h.timezone),
                                      hv);
    }
};

}  // namespace std
#endif  // DATATYPES_DATE_H_

