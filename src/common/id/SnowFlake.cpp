/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/id/SnowFlake.h"

namespace nebula {

int64_t SnowFlake::getId() {
  std::lock_guard<std::mutex> guard(lock_);
  int64_t timestamp = getTimestamp();
  if (timestamp < lastTimestamp_) {
    // TODO
    LOG(FATAL) << "Clock back";
    return sequence_.load();
  }

  // if it is the same time, then the microsecond sequence
  if (lastTimestamp_ == timestamp) {
    sequence_.store((sequence_.load() + 1) & maxSequenceId_);
    // if the microsecond sequence overflow
    if (sequence_.load() == 0) {
      // block to the next millisecond, get the new timestamp
      timestamp = nextTimestamp_();
    }
  } else {
    sequence_.store(0);
  }
  lastTimestamp_ = timestamp;
  return (timestamp - startStmp_) << timestampLeft | workerId_ << machineLeft | sequence_;
}

int64_t SnowFlake::getTimestamp() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t SnowFlake::nextTimestamp_() {
  int64_t timestamp = getTimestamp();
  while (timestamp <= lastTimestamp_) {
    timestamp = getTimestamp();
  }
  return timestamp;
}

}  // namespace nebula
