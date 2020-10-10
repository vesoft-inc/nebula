/* Copyright (c) 2019 vesoft inc. All rights reserved.
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
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec;
}


int64_t WallClock::fastNowInSec() {
    return TscHelper::tickToTimePointInSec(TscHelper::readTsc());
}


int64_t WallClock::slowNowInMilliSec() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}


int64_t WallClock::fastNowInMilliSec() {
    return TscHelper::tickToTimePointInMSec(TscHelper::readTsc());
}


int64_t WallClock::slowNowInMicroSec() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000L + ts.tv_nsec / 1000;
}


int64_t WallClock::fastNowInMicroSec() {
    return TscHelper::tickToTimePointInUSec(TscHelper::readTsc());
}

}  // namespace time
}  // namespace nebula

