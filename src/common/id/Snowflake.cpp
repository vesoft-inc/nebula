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
    // TODO
    LOG(FATAL) << "Clock back";
    return -1;
  }

  // if it is the same time, then the microsecond sequence
  if (lastTimestamp_ == timestamp) {
    sequence_ = (sequence_ + 1) & maxSequence;
    // if the microsecond sequence overflow
    if (sequence_ == 0) {
      // block to the next millisecond, get the new timestamp
      timestamp = nextTimestamp();
    }
  } else {
    lastTimestamp_ = timestamp;
    sequence_ = 0;
  }
  return (timestamp - startStmp) << timestampLeft | workerId_ << workerLeft | sequence_;
}

int64_t Snowflake::getTimestamp() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Snowflake::nextTimestamp() {
  int64_t timestamp = getTimestamp();
  while (timestamp <= lastTimestamp_) {
    timestamp = getTimestamp();
  }
  return timestamp;
}

}  // namespace nebula
