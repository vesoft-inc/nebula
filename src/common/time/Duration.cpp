/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "time/Duration.h"
#include "thread/NamedThread.h"

namespace nebula {
namespace time {

namespace detail {

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


uint64_t calibrateTicksPerUSec() {
    static const std::chrono::steady_clock::time_point kUptime
        = std::chrono::steady_clock::now();
    static const uint64_t kFirstTick = readTsc();

    usleep(10000);

    auto dur = std::chrono::steady_clock::now() - kUptime;
    uint64_t tickDiff = readTsc() - kFirstTick;

    return tickDiff / std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
}


static void launchTickTockThread();

volatile std::atomic<uint64_t> ticksPerUSec{
    []() -> uint64_t {
        launchTickTockThread();
        // re-launch tick-tock thread after forking
        ::pthread_atfork(nullptr, nullptr, &launchTickTockThread);
        return calibrateTicksPerUSec();
    }()
};

void launchTickTockThread() {
    thread::NamedThread t("tick-tock",
        []() {
            while (true) {
                sleep(3);
                ticksPerUSec.store(calibrateTicksPerUSec());
            }  // while
        });

    t.detach();
}

}  // namespace detail


void Duration::reset(bool paused) {
    isPaused_ = paused;
    accumulated_ = 0;
    if (isPaused_) {
        startTick_ = 0;
    } else {
        startTick_ = detail::readTsc();
    }
}


void Duration::pause() {
    if (isPaused_) {
        return;
    }

    isPaused_ = true;
    accumulated_ += (detail::readTsc() - startTick_);
    startTick_ = 0;
}


void Duration::resume() {
    if (!isPaused_) {
        return;
    }

    startTick_ = detail::readTsc();
    isPaused_ = false;
}


uint64_t Duration::elapsedInSec() const {
    uint64_t ticks = isPaused_ ? accumulated_ : detail::readTsc() - startTick_ + accumulated_;
    return (ticks / detail::ticksPerUSec.load() + 500000) / 1000000;
}


uint64_t Duration::elapsedInMSec() const {
    uint64_t ticks = isPaused_ ? accumulated_ : detail::readTsc() - startTick_ + accumulated_;
    return (ticks / detail::ticksPerUSec.load() + 500) / 1000;
}


uint64_t Duration::elapsedInUSec() const {
    uint64_t ticks = isPaused_ ? accumulated_ : detail::readTsc() - startTick_ + accumulated_;
    return ticks / detail::ticksPerUSec.load();
}

}  // namespace time
}  // namespace nebula
