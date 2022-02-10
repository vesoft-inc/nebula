/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_TIME_CONSTANTS_H
#define COMMON_TIME_CONSTANTS_H

namespace nebula {
namespace time {

static constexpr int kDayOfLeapYear = 366;
static constexpr int kDayOfCommonYear = 365;

static constexpr int64_t kSecondsOfMinute = 60;
static constexpr int64_t kSecondsOfHour = 60 * kSecondsOfMinute;
static constexpr int64_t kSecondsOfDay = 24 * kSecondsOfHour;

}  // namespace time
}  // namespace nebula
#endif
