/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/id/Snowflake.h"

namespace nebula {
void Snowflake::initWorkerId(meta::MetaClient* client) {
  const std::string& ip = client->getLocalIp();
  auto result = client->getWorkerId(ip).get();
  if (!result.ok()) {
    LOG(FATAL) << "Failed to get workerId from meta server";
  }
  workerId_ = result.value();
  LOG(INFO) << "WorkerId init success: " << workerId_;
}

int64_t Snowflake::getId() {
  std::lock_guard<std::mutex> guard(mutex_);

  int64_t timestamp = getTimestamp();
  if (timestamp < lastTimestamp_) {
    LOG(ERROR) << "Clock back";
    return kFirstBitRevert & getIdByTs(timestamp);
  }

  return getIdByTs(timestamp);
}

// get snowflake id by timestamp
// update lastTimestamp_ or sequence_
int64_t Snowflake::getIdByTs(int64_t timestamp) {
  // if it is the same time, then the microsecond sequence
  if (lastTimestamp_ == timestamp) {
    sequence_ = (sequence_ + 1) & kMaxSequence;
    // if the microsecond sequence overflow
    if (sequence_ == 0) {
      // block to the next millisecond, get the new timestamp
      timestamp = nextTimestamp();
    }
  } else {
    sequence_ = 0;
  }
  lastTimestamp_ = timestamp;
  return (timestamp - kStartStmp) << kTimestampLeft | workerId_ << kWorkerIdLeft | sequence_;
}

int64_t Snowflake::getTimestamp() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// wait for the next millisecond
int64_t Snowflake::nextTimestamp() {
  int64_t timestamp = getTimestamp();
  while (timestamp <= lastTimestamp_) {
    timestamp = getTimestamp();
  }
  return timestamp;
}

}  // namespace nebula
