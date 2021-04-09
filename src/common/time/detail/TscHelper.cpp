/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <time.h>
#include "common/thread/NamedThread.h"
#include "common/time/detail/TscHelper.h"

namespace nebula {
namespace time {

#if defined(__x86_64__)
TscHelper::TscHelper() {
    auto ret = clock_gettime(CLOCK_REALTIME, &startRealTime_);
    DCHECK_EQ(0, ret);
    startMonoTime_ = std::chrono::steady_clock::now();
    firstTick_ = readTscImpl();
    ::usleep(10000);
    calibrate();
    thread::NamedThread calibrator("tsc-calibrator",
        [this] () {
            while (true) {
                sleep(2);
                calibrate();
            }
        });
    /**
     * Detach the thread so we can avoid the joining latency introduced by `sleep'.
     * This shall be safe because all data this thread will access are static POD.
     */
    calibrator.detach();
}


TscHelper& TscHelper::get() {
    static TscHelper tscHelper;
    return tscHelper;
}


uint64_t TscHelper::readTsc() {
    return get().readTscImpl();
}

uint64_t TscHelper::readTscImpl() {
#ifdef DURATION_USE_RDTSCP
    uint32_t eax, ecx, edx;
    __asm__ volatile ("rdtscp" : "=a" (eax), "=d" (edx) : "c");
#else
    uint32_t eax, edx;
    __asm__ volatile ("rdtsc" : "=a" (eax), "=d" (edx));
#endif
    return ((uint64_t)edx) << 32 | (uint64_t)eax;
}


void TscHelper::calibrate() {
    auto dur = std::chrono::steady_clock::now() - startMonoTime_;
    uint64_t tickDiff = readTscImpl() - firstTick_;

    uint64_t ticksPerUSec =
        tickDiff / std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
    ticksPerSecFactor_ = 1.0 / ticksPerUSec / 1000000.0;
    ticksPerMSecFactor_ = 1.0 / ticksPerUSec / 1000.0;
    ticksPerUSecFactor_ = 1.0 / ticksPerUSec;
}


uint64_t TscHelper::ticksToDurationInSec(uint64_t ticks) {
    return ticks * get().ticksPerSecFactor_ + 0.5;
}


uint64_t TscHelper::ticksToDurationInMSec(uint64_t ticks) {
    return ticks * get().ticksPerMSecFactor_ + 0.5;
}


uint64_t TscHelper::ticksToDurationInUSec(uint64_t ticks) {
    return ticks * get().ticksPerUSecFactor_;
}


uint64_t TscHelper::tickToTimePointInSec(uint64_t tick) {
    static const int64_t st = get().startRealTime_.tv_sec;
    return st + get().ticksToDurationInSec(tick - get().firstTick_);
}


uint64_t TscHelper::tickToTimePointInMSec(uint64_t tick) {
    static const int64_t st = get().startRealTime_.tv_sec * 1000
                                + get().startRealTime_.tv_nsec / 1000000;
    return st + get().ticksToDurationInMSec(tick - get().firstTick_);
}


uint64_t TscHelper::tickToTimePointInUSec(uint64_t tick) {
    static const int64_t st = get().startRealTime_.tv_sec * 1000000
                                + get().startRealTime_.tv_nsec / 1000;
    return st + get().ticksToDurationInUSec(tick - get().firstTick_);
}

#endif  // defined(__x86_64__)


}  // namespace time
}  // namespace nebula
