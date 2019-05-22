/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "time/detail/TscHelper.h"
#include "time/WallClock.h"

namespace nebula {
namespace time {

int64_t WallClock::slowNowInSec() {
    struct timespec ts;
    CHECK_EQ(0, clock_gettime(CLOCK_REALTIME, &ts));
    return ts.tv_sec;
}


int64_t WallClock::fastNowInSec() {
    static const int64_t st = kStartTime.tv_sec;
    return (readTsc() - kFirstTick) / ticksPerUSec / 1000000 + st;
}


int64_t WallClock::slowNowInMilliSec() {
    struct timespec ts;
    CHECK_EQ(0, clock_gettime(CLOCK_REALTIME, &ts));
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}


int64_t WallClock::fastNowInMilliSec() {
    static const int64_t st = kStartTime.tv_sec * 1000 + kStartTime.tv_nsec / 1000000;
    return (readTsc() - kFirstTick) / ticksPerUSec / 1000 + st;
}


int64_t WallClock::slowNowInMicroSec() {
    struct timespec ts;
    CHECK_EQ(0, clock_gettime(CLOCK_REALTIME, &ts));
    return ts.tv_sec * 1000000L + ts.tv_nsec / 1000;
}


int64_t WallClock::fastNowInMicroSec() {
    static const int64_t st = kStartTime.tv_sec * 1000000 + kStartTime.tv_nsec / 1000;
    return (readTsc() - kFirstTick) / ticksPerUSec + st;
}

}  // namespace time
}  // namespace nebula

