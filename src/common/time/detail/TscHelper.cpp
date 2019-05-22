/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "thread/NamedThread.h"
#include "time/detail/TscHelper.h"

namespace nebula {
namespace time {

namespace {

uint64_t calibrateTicksPerUSec() {
    auto dur = std::chrono::steady_clock::now() - kUptime;
    uint64_t tickDiff = readTsc() - kFirstTick;

    return tickDiff / std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
}


void launchTickTockThread() {
    thread::NamedThread t(
        "tick-tock",
        [] {
            while (true) {
                sleep(2);
                ticksPerUSec.store(calibrateTicksPerUSec());
            }  // while
        });
    t.detach();
}

}  // Anonymous namespace


volatile uint64_t readTsc() {
#ifdef DURATION_USE_RDTSCP
    uint32_t eax, ecx, edx;
    __asm__ volatile ("rdtscp" : "=a" (eax), "=d" (edx) : "c");
#else
    uint32_t eax, edx;
    __asm__ volatile ("rdtsc" : "=a" (eax), "=d" (edx));
#endif
    return ((uint64_t)edx) << 32 | (uint64_t)eax;
}


const struct timespec kStartTime = [] {
    struct timespec st;
    auto res = clock_gettime(CLOCK_REALTIME, &st);
    DCHECK_EQ(0, res);
    return st;
}();
const std::chrono::steady_clock::time_point kUptime = std::chrono::steady_clock::now();
const uint64_t kFirstTick = readTsc();

volatile std::atomic<uint64_t> ticksPerUSec{[] {
    launchTickTockThread();
    // re-launch tick-tock thread after forking
    ::pthread_atfork(nullptr, nullptr, &launchTickTockThread);

    usleep(10000);

    return calibrateTicksPerUSec();
}()};

}  // namespace time
}  // namespace nebula

