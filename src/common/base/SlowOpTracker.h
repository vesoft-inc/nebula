/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_BASE_SLOWOPTRACKER_H_
#define COMMON_BASE_SLOWOPTRACKER_H_

#include <gflags/gflags_declare.h>  // for DECLARE_int64
#include <stdint.h>                 // for int64_t

#include <string>  // for operator<<, char_traits, string

#include "common/base/Base.h"
#include "common/base/Logging.h"    // for LOG, LogMessage, _LOG_INFO
#include "common/time/WallClock.h"  // for WallClock

DECLARE_int64(slow_op_threshold_ms);

namespace nebula {

class SlowOpTracker {
 public:
  SlowOpTracker() : startMs_(time::WallClock::fastNowInMilliSec()) {}

  ~SlowOpTracker() = default;

  bool slow(int64_t threshold = 0) {
    dur_ = time::WallClock::fastNowInMilliSec() - startMs_;
    if (dur_ < 0) {
      dur_ = 0;
    }
    return threshold > 0 ? dur_ > threshold : dur_ > FLAGS_slow_op_threshold_ms;
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
