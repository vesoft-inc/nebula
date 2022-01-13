/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/datatypes/Date.h"

#include <folly/String.h>
#include <folly/hash/Hash.h>

#include <cstdint>

namespace nebula {

const int64_t kDaysSoFar[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
const int64_t kLeapDaysSoFar[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

int8_t dayOfMonth(int16_t year, int8_t month) {
  return isLeapYear(year) ? kLeapDaysSoFar[month] - kLeapDaysSoFar[month - 1]
                          : kDaysSoFar[month] - kDaysSoFar[month - 1];
}

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

void Date::addDuration(const Duration& duration) {
  int64_t tmp{0}, carry{0};
  tmp = month + duration.months;
  if (std::abs(tmp) > 12) {
    carry = tmp / 12;
    month = tmp % 12;
  } else {
    month = tmp;
  }
  if (month <= 0) {
    carry--;
    month += 12;
  }
  year += carry;

  tmp = day + duration.days();
  if (tmp > 0) {
    int8_t dom = dayOfMonth(year, month);
    while (tmp > dom) {
      tmp -= dom;
      month += 1;
      if (month > 12) {
        year += 1;
        month = 1;
      }
      dom = dayOfMonth(year, month);
    }
  } else {
    int8_t dom = (month == 1 ? dayOfMonth(year - 1, 12) : dayOfMonth(year, month - 1));
    while (tmp <= 0) {
      tmp += dom;
      month -= 1;
      if (month <= 0) {
        year--;
        month += 12;
      }
      dom = (month == 1 ? dayOfMonth(year - 1, 12) : dayOfMonth(year, month - 1));
    }
  }
  day = tmp;
}

void Date::subDuration(const Duration& duration) {
  return addDuration(-duration);
}

std::string Date::toString() const {
  // It's in current timezone already
  return folly::stringPrintf("%d-%02d-%02d", year, month, day);
}

void Time::addDuration(const Duration& duration) {
  int64_t tmp{0}, carry{0};
  tmp = microsec + duration.microsecondsInSecond();
  if (std::abs(tmp) >= 1000000) {
    carry = tmp / 1000000;
    microsec = tmp % 1000000;
  } else {
    microsec = tmp;
  }
  if (microsec < 0) {
    carry--;
    microsec += 1000000;
  }
  tmp = sec + duration.seconds + carry;
  carry = 0;
  if (std::abs(tmp) >= 60) {
    carry = tmp / 60;
    sec = tmp % 60;
  } else {
    sec = tmp;
  }
  if (sec < 0) {
    carry--;
    sec += 60;
  }
  tmp = minute + carry;
  carry = 0;
  if (std::abs(tmp) >= 60) {
    carry = tmp / 60;
    minute = tmp % 60;
  } else {
    minute = tmp;
  }
  if (minute < 0) {
    carry--;
    minute += 60;
  }
  tmp = hour + carry;
  carry = 0;
  if (std::abs(tmp) >= 24) {
    carry = tmp / 24;
    hour = tmp % 24;
  } else {
    hour = tmp;
  }
  if (hour < 0) {
    carry--;
    hour += 24;
  }
}

void Time::subDuration(const Duration& duration) {
  addDuration(-duration);
}

std::string Time::toString() const {
  return folly::sformat("{:0>2}:{:0>2}:{:0>2}.{:0>6}000",
                        static_cast<uint8_t>(hour),
                        static_cast<uint8_t>(minute),
                        static_cast<uint8_t>(sec),
                        static_cast<uint32_t>(microsec));
}

void DateTime::addDuration(const Duration& duration) {
  // The origin fields of DateTime is unsigned, but there will be some negative intermediate results
  // so I define some variable(field, tYear, tMonth) for this.
  int64_t tmp{0}, carry{0}, field{0};
  tmp = month + duration.months;
  if (std::abs(tmp) > 12) {
    carry = tmp / 12;
    /*month*/ field = tmp % 12;
  } else {
    /*month*/ field = tmp;
  }
  if (/*month*/ field <= 0) {
    carry--;
    /*month*/ field += 12;
  }
  year += carry;
  carry = 0;
  month = field;
  field = 0;

  tmp = microsec + duration.microsecondsInSecond();
  if (std::abs(tmp) >= 1000000) {
    carry = tmp / 1000000;
    /*microsec*/ field = tmp % 1000000;
  } else {
    /*microsec*/ field = tmp;
  }
  if (/*microsec*/ field < 0) {
    carry--;
    /*microsec*/ field += 1000000;
  }
  microsec = field;
  field = 0;
  tmp = sec + duration.seconds + carry;
  carry = 0;
  if (std::abs(tmp) >= 60) {
    carry = tmp / 60;
    /*sec*/ field = tmp % 60;
  } else {
    /*sec*/ field = tmp;
  }
  if (/*sec*/ field < 0) {
    carry--;
    /*sec*/ field += 60;
  }
  sec = field;
  field = 0;
  tmp = minute + carry;
  carry = 0;
  if (std::abs(tmp) >= 60) {
    carry = tmp / 60;
    /*minute*/ field = tmp % 60;
  } else {
    /*minute*/ field = tmp;
  }
  if (/*minute*/ field < 0) {
    carry--;
    /*minute*/ field += 60;
  }
  minute = field;
  field = 0;
  tmp = hour + carry;
  carry = 0;
  if (std::abs(tmp) >= 24) {
    carry = tmp / 24;
    /*hour*/ field = tmp % 24;
  } else {
    /*hour*/ field = tmp;
  }
  if (/*hour*/ field < 0) {
    carry--;
    /*hour*/ field += 24;
  }
  hour = field;
  field = 0;

  tmp = day + carry;
  carry = 0;
  if (tmp > 0) {
    int8_t dom = dayOfMonth(year, month);
    while (tmp > dom) {
      tmp -= dom;
      month += 1;
      if (month > 12) {
        year += 1;
        month = 1;
      }
      dom = dayOfMonth(year, month);
    }
  } else {
    int8_t dom = (month == 1 ? dayOfMonth(year - 1, 12) : dayOfMonth(year, month - 1));
    int64_t tMonth = month;
    int64_t tYear = year;
    while (tmp <= 0) {
      tmp += dom;
      /*month*/ tMonth -= 1;
      if (/*month*/ tMonth <= 0) {
        /*year*/ tYear--;
        /*month*/ tMonth += 12;
      }
      dom = (/*month*/ tMonth == 1 ? dayOfMonth(/*year*/ tYear - 1, 12)
                                   : dayOfMonth(/*year*/ tYear, /*month*/ tMonth - 1));
    }
    month = tMonth;
    year = tYear;
  }
  day = tmp;
}

void DateTime::subDuration(const Duration& duration) {
  return addDuration(-duration);
}

std::string DateTime::toString() const {
  // It's in current timezone already
  return folly::sformat("{}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}.{:0>6}000",
                        static_cast<int16_t>(year),
                        static_cast<uint8_t>(month),
                        static_cast<uint8_t>(day),
                        static_cast<uint8_t>(hour),
                        static_cast<uint8_t>(minute),
                        static_cast<uint8_t>(sec),
                        static_cast<uint32_t>(microsec));
}

}  // namespace nebula

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
  return folly::hash::fnv64_buf(reinterpret_cast<const void*>(&h.microsec), sizeof(h.microsec), hv);
}

std::size_t hash<nebula::DateTime>::operator()(const nebula::DateTime& h) const noexcept {
  return h.qword;
}

}  // namespace std
