/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/time/detail/TscHelper.h"
#include "common/time/WallClock.h"

namespace nebula {
namespace time {

int64_t WallClock::slowNowInSec() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec;
}


int64_t WallClock::slowNowInMilliSec() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}


int64_t WallClock::slowNowInMicroSec() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1000000L + ts.tv_nsec / 1000;
}


#if defined(__x86_64__)
int64_t WallClock::fastNowInSec() {
    return TscHelper::tickToTimePointInSec(TscHelper::readTsc());
}


int64_t WallClock::fastNowInMilliSec() {
    return TscHelper::tickToTimePointInMSec(TscHelper::readTsc());
}


int64_t WallClock::fastNowInMicroSec() {
    return TscHelper::tickToTimePointInUSec(TscHelper::readTsc());
}
#elif defined(__aarch64__) || defined(__arm64__) || defined(__mips64)
int64_t WallClock::fastNowInSec() {
    return WallClock::slowNowInSec();
}


int64_t WallClock::fastNowInMilliSec() {
    return slowNowInMilliSec();
}


int64_t WallClock::fastNowInMicroSec() {
    return slowNowInMicroSec();
}
#else   // defined(__x86_64__)
#error "Only x86_64 and aarch64 are supported"
#endif  // defined(__x86_64__)

}  // namespace time
}  // namespace nebula

