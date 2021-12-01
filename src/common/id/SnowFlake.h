/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/utils/Utils.h"

namespace nebula {
namespace meta {

class SnowFlake {
 public:
  explicit SnowFlake(MetaClient* metaClient) : metaClient_(metaClient) {}

  // TODO: return value may be bool?
  void init() {
    const std::string& ip = metaClient_->getLocalIp();

    auto result = metaClient_->getWorkerId(nebula::Utils::getMacAddr(ip));
    if (!result.ok()) {
      LOG(FATAL) << "Failed to get worker id: " << result.status();
    }
    int32_t workerId = result.value();
    if (workerId < 0 || workerId > max_machine_id_) {
      LOG(FATAL) << "workerId should be in [0, 1023]";
    }
  }

  int64_t getId() {
    std::lock_guard<std::mutex> guard(lock_);

    int64_t timestamp = getTimestamp();
    if (timestamp < lastTimestamp_) {
      // TODO
      // Clock back
      return sequence_;
    }

    // if it is the same time, then the microsecond sequence
    if (lastTimestamp_ == timestamp) {
      sequence_ = (sequence_ + 1) & max_sequence_id_;
      // if the microsecond sequence overflow
      if (sequence_ == 0) {
        // block to the next millisecond, get the new timestamp
        timestamp = nextTimestamp_();
      }
    } else {
      sequence_ = 0;
    }
    lastTimestamp_ = timestamp;
    return (timestamp - start_stmp_) << timestamp_left | workerId_ << machine_left | sequence_;
  }

 private:
  mutable std::mutex lock_;

  meta::MetaClient* metaClient_;

  int64_t workerId_;            // 10 bits
  int64_t lastTimestamp_ = -1;  // 41 bits
  int64_t sequence_ = 0;        // 12 bits

  int64_t getTimestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  int64_t nextTimestamp_() {
    int64_t timestamp = getTimestamp();
    while (timestamp <= lastTimestamp_) {
      timestamp = getTimestamp();
    }
    return timestamp;
  }

  // start
  static constexpr int64_t start_stmp_ = 1480166465631;
  static constexpr int64_t sequence_bit_ = 12;
  static constexpr int64_t worker_bit_ = 10;

  static constexpr int64_t max_machine_id_ = (1 << worker_bit_) - 1;
  static constexpr int64_t max_sequence_id_ = (1 << sequence_bit_) - 1;

  static constexpr int64_t machine_left = sequence_bit_;
  static constexpr int64_t timestamp_left = sequence_bit_ + worker_bit_;
};  // namespace meta

}  // namespace meta
}  // namespace nebula
