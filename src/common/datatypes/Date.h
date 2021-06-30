/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_DATE_H_
#define COMMON_DATATYPES_DATE_H_

#include <string>

namespace nebula {

// In nebula only store UTC time, and the interpretion of time value based on the
// timezone configuration in current system.

extern const int64_t kDaysSoFar[];
extern const int64_t kLeapDaysSoFar[];

struct Date {
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

    void __clear() {
        clear();
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

    bool operator<(const Date& rhs) const {
        if (!(year == rhs.year)) {
            return year < rhs.year;
        }
        if (!(month == rhs.month)) {
            return month < rhs.month;
        }
        if (!(day == rhs.day)) {
            return day < rhs.day;
        }
        return false;
    }

    Date operator+(int64_t days) const;
    Date operator-(int64_t days) const;

    std::string toString() const;

    // Return the number of days since -32768/1/1
    int64_t toInt() const;
    // Convert the number of days since -32768/1/1 to the real date
    void fromInt(int64_t days);
};

inline std::ostream &operator<<(std::ostream& os, const Date& d) {
    os << d.toString();
    return os;
}

struct Time {
    int8_t hour;
    int8_t minute;
    int8_t sec;
    int32_t microsec;

    Time() : hour{0}, minute{0}, sec{0}, microsec{0} {}
    Time(int8_t h, int8_t min, int8_t s, int32_t us)
        : hour{h}, minute{min}, sec{s}, microsec{us} {}

    void clear() {
        hour = 0;
        minute = 0;
        sec = 0;
        microsec = 0;
    }

    void __clear() {
        clear();
    }

    bool operator==(const Time& rhs) const {
        return hour == rhs.hour &&
               minute == rhs.minute &&
               sec == rhs.sec &&
               microsec == rhs.microsec;
    }

    bool operator<(const Time& rhs) const {
        if (!(hour == rhs.hour)) {
            return hour < rhs.hour;
        }
        if (!(minute == rhs.minute)) {
            return minute < rhs.minute;
        }
        if (!(sec == rhs.sec)) {
            return sec < rhs.sec;
        }
        if (!(microsec == rhs.microsec)) {
            return microsec < rhs.microsec;
        }
        return false;
    }

    std::string toString() const;
};

inline std::ostream &operator<<(std::ostream& os, const Time& d) {
    os << d.toString();
    return os;
}

struct DateTime {
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif  // defined(__GNUC__)
    union {
        struct {
            int64_t year:16;
            uint64_t month:4;
            uint64_t day:5;
            uint64_t hour:5;
            uint64_t minute:6;
            uint64_t sec:6;
            uint64_t microsec:22;
        };
        uint64_t qword;
    };
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif  // defined(__GNUC__)

    DateTime() : year{0}, month{1}, day{1}, hour{0}, minute{0}, sec{0}, microsec{0} {}
    DateTime(int16_t y, int8_t m, int8_t d, int8_t h, int8_t min, int8_t s, int32_t us) {
        year = y;
        month = m;
        day = d;
        hour = h;
        minute = min;
        sec = s;
        microsec = us;
    }
    explicit DateTime(const Date &date) {
        year = date.year;
        month = date.month;
        day = date.day;
        hour = 0;
        minute = 0;
        sec = 0;
        microsec = 0;
    }

    void clear() {
        year = 0;
        month = 1;
        day = 1;
        hour = 0;
        minute = 0;
        sec = 0;
        microsec = 0;
    }

    void __clear() {
        clear();
    }

    bool operator==(const DateTime& rhs) const {
        return year == rhs.year &&
               month == rhs.month &&
               day == rhs.day &&
               hour == rhs.hour &&
               minute == rhs.minute &&
               sec == rhs.sec &&
               microsec == rhs.microsec;
    }
    bool operator<(const DateTime& rhs) const {
        if (!(year == rhs.year)) {
            return year < rhs.year;
        }
        if (!(month == rhs.month)) {
            return month < rhs.month;
        }
        if (!(day == rhs.day)) {
            return day < rhs.day;
        }
        if (!(hour == rhs.hour)) {
            return hour < rhs.hour;
        }
        if (!(minute == rhs.minute)) {
            return minute < rhs.minute;
        }
        if (!(sec == rhs.sec)) {
            return sec < rhs.sec;
        }
        if (!(microsec == rhs.microsec)) {
            return microsec < rhs.microsec;
        }
        return false;
    }

    std::string toString() const;
};


inline std::ostream &operator<<(std::ostream& os, const DateTime& d) {
    os << d.toString();
    return os;
}

}  // namespace nebula


namespace std {

// Inject a customized hash function
template<>
struct hash<nebula::Date> {
    std::size_t operator()(const nebula::Date& h) const noexcept;
};

template<>
struct hash<nebula::Time> {
    std::size_t operator()(const nebula::Time& h) const noexcept;
};

template<>
struct hash<nebula::DateTime> {
    std::size_t operator()(const nebula::DateTime& h) const noexcept;
};

}  // namespace std
#endif  // COMMON_DATATYPES_DATE_H_
