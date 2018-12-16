/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_TIME_DURATION_H_
#define COMMON_TIME_DURATION_H_

#include "base/Base.h"

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
    uint64_t startTick_;
};

}  // namespace time
}  // namespace nebula

#endif  // COMMON_TIME_DURATION_H_

