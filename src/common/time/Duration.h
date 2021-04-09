/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_TIME_DURATION_H_
#define COMMON_TIME_DURATION_H_

#include "common/base/Base.h"

namespace nebula {
namespace time {

class Duration final {
public:
    explicit Duration(bool paused = false) {
        reset(paused);
    }

    void reset(bool paused = false);

    void pause();
    void resume();

    bool isPaused() const {
        return isPaused_;
    }

    uint64_t elapsedInSec() const;
    uint64_t elapsedInMSec() const;
    uint64_t elapsedInUSec() const;

private:
    bool isPaused_;
    uint64_t accumulated_;
#if defined(__x86_64__)
    uint64_t startTick_;
#else
    struct timespec startTick_;
    uint64_t nanoDiff(struct timespec start, struct timespec end) const;
    struct timespec now() const;
#endif
};

}  // namespace time
}  // namespace nebula

#endif  // COMMON_TIME_DURATION_H_

