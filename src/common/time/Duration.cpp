/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/time/detail/TscHelper.h"
#include "common/time/Duration.h"

namespace nebula {
namespace time {

#if defined(__x86_64__)
void Duration::reset(bool paused) {
    isPaused_ = paused;
    accumulated_ = 0;
    if (isPaused_) {
        startTick_ = 0;
    } else {
        startTick_ = TscHelper::readTsc();
    }
}


void Duration::pause() {
    if (isPaused_) {
        return;
    }

    isPaused_ = true;
    accumulated_ += (TscHelper::readTsc() - startTick_);
    startTick_ = 0;
}


void Duration::resume() {
    if (!isPaused_) {
        return;
    }

    startTick_ = TscHelper::readTsc();
    isPaused_ = false;
}


uint64_t Duration::elapsedInSec() const {
    uint64_t ticks = isPaused_ ? accumulated_ :
                                 TscHelper::readTsc() - startTick_ + accumulated_;
    return TscHelper::ticksToDurationInSec(ticks);
}


uint64_t Duration::elapsedInMSec() const {
    uint64_t ticks = isPaused_ ? accumulated_ :
                                 TscHelper::readTsc() - startTick_ + accumulated_;
    return TscHelper::ticksToDurationInMSec(ticks);
}


uint64_t Duration::elapsedInUSec() const {
    uint64_t ticks = isPaused_ ? accumulated_ :
                                 TscHelper::readTsc() - startTick_ + accumulated_;
    return TscHelper::ticksToDurationInUSec(ticks);
}
#else
struct timespec Duration::now() const {
    struct timespec now;
    ::clock_gettime(CLOCK_MONOTONIC, &now);
    return now;
}


uint64_t Duration::nanoDiff(struct timespec start, struct timespec end) const {
    struct timespec duration;
    if (end.tv_nsec < start.tv_nsec) {
        DCHECK(end.tv_sec > start.tv_sec);
        end.tv_nsec += 1000000000L;
        end.tv_sec -= 1;
    }
    duration.tv_sec = end.tv_sec - start.tv_sec;
    duration.tv_nsec = end.tv_nsec - start.tv_nsec;

    return duration.tv_sec * 1000000000L + duration.tv_nsec;
}


void Duration::reset(bool paused) {
    isPaused_ = paused;
    accumulated_ = 0;
    if (isPaused_) {
        ::memset(&startTick_, 0, sizeof(startTick_));
    } else {
        startTick_ = now();
    }
}


void Duration::pause() {
    if (isPaused_) {
        return;
    }

    isPaused_ = true;
    accumulated_ += nanoDiff(startTick_, now());
    ::memset(&startTick_, 0, sizeof(startTick_));
}


void Duration::resume() {
    if (!isPaused_) {
        return;
    }

    startTick_ = now();
    isPaused_ = false;
}


uint64_t Duration::elapsedInSec() const {
    uint64_t nsec = isPaused_ ? accumulated_ :
                                nanoDiff(startTick_, now()) + accumulated_;
    return nsec / 1000000000L;
}


uint64_t Duration::elapsedInMSec() const {
    uint64_t nsec = isPaused_ ? accumulated_ :
                                nanoDiff(startTick_, now()) + accumulated_;
    return nsec / 1000000L;
}


uint64_t Duration::elapsedInUSec() const {
    uint64_t nsec = isPaused_ ? accumulated_ :
                                nanoDiff(startTick_, now()) + accumulated_;
    return nsec / 1000L;
}
#endif

}  // namespace time
}  // namespace nebula
