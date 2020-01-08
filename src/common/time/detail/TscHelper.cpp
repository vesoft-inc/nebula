/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <time.h>
#include "thread/NamedThread.h"
#include "time/detail/TscHelper.h"

namespace nebula {
namespace time {

TscHelper::TscHelper() {
    auto ret = clock_gettime(CLOCK_REALTIME, &kStartTime);
    DCHECK_EQ(0, ret);
    kUptime = std::chrono::steady_clock::now();
    kFirstTick = readTscImpl();
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
    auto dur = std::chrono::steady_clock::now() - kUptime;
    uint64_t tickDiff = readTscImpl() - kFirstTick;

    uint64_t ticksPerUSec =
        tickDiff / std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
    ticksPerSecFactor = 1.0 / ticksPerUSec / 1000000.0;
    ticksPerMSecFactor = 1.0 / ticksPerUSec / 1000.0;
    ticksPerUSecFactor = 1.0 / ticksPerUSec;
}


uint64_t TscHelper::ticksToDurationInSec(uint64_t ticks) {
    return ticks * get().ticksPerSecFactor + 0.5;
}


uint64_t TscHelper::ticksToDurationInMSec(uint64_t ticks) {
    return ticks * get().ticksPerMSecFactor + 0.5;
}


uint64_t TscHelper::ticksToDurationInUSec(uint64_t ticks) {
    return ticks * get().ticksPerUSecFactor;
}


uint64_t TscHelper::tickToTimePointInSec(uint64_t tick) {
    static const int64_t st = get().kStartTime.tv_sec;
    return st + get().ticksToDurationInSec(tick - get().kFirstTick);
}


uint64_t TscHelper::tickToTimePointInMSec(uint64_t tick) {
    static const int64_t st = get().kStartTime.tv_sec * 1000
                                + get().kStartTime.tv_nsec / 1000000;
    return st + get().ticksToDurationInMSec(tick - get().kFirstTick);
}


uint64_t TscHelper::tickToTimePointInUSec(uint64_t tick) {
    static const int64_t st = get().kStartTime.tv_sec * 1000000
                                + get().kStartTime.tv_nsec / 1000;
    return st + get().ticksToDurationInUSec(tick - get().kFirstTick);
}


}  // namespace time
}  // namespace nebula
