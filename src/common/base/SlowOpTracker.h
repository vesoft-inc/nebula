/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_SLOWOPTRACKER_H_
#define COMMON_BASE_SLOWOPTRACKER_H_

#include "common/base/Base.h"
#include "common/time/WallClock.h"

DECLARE_int64(slow_op_threshhold_ms);

namespace nebula {

class SlowOpTracker {
public:
    SlowOpTracker()
        : startMs_(time::WallClock::fastNowInMilliSec()) {}

    ~SlowOpTracker() = default;

    bool slow(int64_t threshhold = 0) {
        dur_ = time::WallClock::fastNowInMilliSec() - startMs_;
        if (dur_ < 0) {
            dur_ = 0;
        }
        return threshhold > 0 ? dur_ > threshhold : dur_ > FLAGS_slow_op_threshhold_ms;
    }

    void output(const std::string& prefix, const std::string& msg) {
        LOG(INFO) << prefix << "total time:" << dur_ << "ms, " << msg;
    }

private:
    int64_t startMs_ = 0;
    int64_t dur_ = 0;
};

}  // namespace nebula
#endif  // COMMON_BASE_SLOWOPTRACKER_H_
