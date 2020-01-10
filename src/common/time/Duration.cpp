/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "time/detail/TscHelper.h"
#include "time/Duration.h"

namespace nebula {
namespace time {

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

}  // namespace time
}  // namespace nebula
