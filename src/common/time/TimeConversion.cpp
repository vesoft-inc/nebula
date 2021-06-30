/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/TimeConversion.h"

namespace nebula {
namespace time {

const DateTime TimeConversion::kEpoch(1970, 1, 1, 0, 0, 0, 0);

/*static*/ int64_t TimeConversion::dateTimeDiffSeconds(const DateTime &dateTime0,
                                                       const DateTime &dateTime1) {
    // check the negative divide result, it's used in the negative year number
    // computing.
    static_assert(-1 / 2 == 0, "");
    // Year Base Verification
    static_assert(0 % 100 == 0, "");

    // Compute intervening leap days correctly even if year is negative.
    // take care to avoid integer overflow here.
    int a4 = shr(dateTime0.year, 2) + shr(0, 2) - !(dateTime0.year & 3);
    int b4 = shr(dateTime1.year, 2) + shr(0, 2) - !(dateTime1.year & 3);
    int a100 = (a4 + (a4 < 0)) / 25 - (a4 < 0);
    int b100 = (b4 + (b4 < 0)) / 25 - (b4 < 0);
    int a400 = shr(a100, 2);
    int b400 = shr(b100, 2);
    int intervening_leap_days = (a4 - b4) - (a100 - b100) + (a400 - b400);

    /* Compute the desired time without overflowing.  */
    int64_t years = dateTime0.year - dateTime1.year;
    int64_t days = 365 * years +
                   (isLeapYear(dateTime0.year) ? kLeapDaysSoFar[dateTime0.month - 1]
                                               : kDaysSoFar[dateTime0.month - 1]) -
                   (isLeapYear(dateTime1.year) ? kLeapDaysSoFar[dateTime1.month - 1]
                                               : kDaysSoFar[dateTime1.month - 1]) +
                   dateTime0.day - dateTime1.day + intervening_leap_days;
    int64_t hours = 24 * days + dateTime0.hour - dateTime1.hour;
    int64_t minutes = 60 * hours + dateTime0.minute - dateTime1.minute;
    int64_t seconds = 60 * minutes + dateTime0.sec - dateTime1.sec;
    return seconds;
}

/*static*/ DateTime TimeConversion::unixSecondsToDateTime(int64_t seconds) {
    DateTime dt;
    int64_t days, rem, y;
    const int64_t *ip;

    days = seconds / kSecondsOfDay;
    rem = seconds % kSecondsOfDay;
    while (rem < 0) {
        rem += kSecondsOfDay;
        --days;
    }
    while (rem >= kSecondsOfDay) {
        rem -= kSecondsOfDay;
        ++days;
    }
    dt.hour = rem / kSecondsOfHour;
    rem %= kSecondsOfHour;
    dt.minute = rem / kSecondsOfMinute;
    dt.sec = rem % kSecondsOfMinute;
    y = 1970;

#define DIV(a, b) ((a) / (b) - ((a) % (b) < 0))
#define LEAPS_THRU_END_OF(y) (DIV(y, 4) - DIV(y, 100) + DIV(y, 400))

    while (days < 0 || days >= (isLeapYear(y) ? kDayOfLeapYear : kDayOfCommonYear)) {
        /* Guess a corrected year, assuming 365 days per year.  */
        int64_t yg = y + days / kDayOfCommonYear - (days % kDayOfCommonYear < 0);

        /* Adjust DAYS and Y to match the guessed year.  */
        days -=
            ((yg - y) * kDayOfCommonYear + LEAPS_THRU_END_OF(yg - 1) - LEAPS_THRU_END_OF(y - 1));
        y = yg;
    }
    dt.year = y;
    if (dt.year != y) {
        // overflow
    }
    ip = (isLeapYear(y) ? kLeapDaysSoFar : kDaysSoFar);
    for (y = 11; days < ip[y]; --y) {
        continue;
    }
    days -= ip[y];
    dt.month = y + 1;
    dt.day = days + 1;
    return dt;
}

#undef DIV
#undef LEAPS_THRU_END_OF


}  // namespace time
}  // namespace nebula
