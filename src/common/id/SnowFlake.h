/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/id/IdGenerator.h"

namespace nebula {
namespace meta {

class SnowFlake : public IdGenerator {
 public:
  SnowFlake() = default;

  void init() {
    int64_t machineId = meta_client.getMachineId();
    int64_t datacenterId = meta_client.getDatacenterId();
    if (machineId < 0 || machineId > max_machine_id_) {
      // throw std::invalid_argument("machineId should be in [0, 1023]");
    }
    if (datacenterId < 0 || datacenterId > max_datacenter_id_) {
      // throw std::invalid_argument("datacenterId should be in [0, 31]");
    }
  }

  int64_t getId() {
    // lock

    int64_t timestamp = getTimestamp();
    if (timestamp < lastTimestamp_) {
      // TODO exception
      return sequence_;
    }

    // 如果是同一时间生成的，则进行毫秒内序列
    if (lastTimestamp_ == timestamp) {
      sequence_ = (sequence_ + 1) & max_sequence_id_;
      // 毫秒内序列溢出
      if (sequence_ == 0) {
        // 阻塞到下一个毫秒,获得新的时间戳
        timestamp = nextTimestamp_();
      }
    } else {
      sequence_ = 0;
    }
    lastTimestamp_ = timestamp;
    return (timestamp - start_stmp_) << timestamp_left | datacenterId_ << datacenter_left |
           machineId_ << machine_left | sequence_;
  }

 private:
  meta::MetaClient meta_client;

  int64_t machineId_;           // 5 bits
  int64_t datacenterId_;        // 5 bits
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
  static constexpr int64_t machine_bit_ = 5;
  static constexpr int64_t datacenter_bit_ = 5;

  static constexpr int64_t max_machine_id_ = (1 << machine_bit_) - 1;
  static constexpr int64_t max_datacenter_id_ = (1 << datacenter_bit_) - 1;
  static constexpr int64_t max_sequence_id_ = (1 << sequence_bit_) - 1;

  static constexpr int64_t machine_left = sequence_bit_;
  static constexpr int64_t datacenter_left = sequence_bit_ + machine_bit_;
  static constexpr int64_t timestamp_left = sequence_bit_ + machine_bit_ + datacenter_bit_;
};  // namespace meta

}  // namespace meta
}  // namespace nebula
