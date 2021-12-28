/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_ID_SNOWFLAKE_H_
#define COMMON_ID_SNOWFLAKE_H_

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"

namespace nebula {
class Snowflake {
  FRIEND_TEST(SnowflakeTest, TestWorkerId);
  FRIEND_TEST(SnowflakeTest, TestConcurrency);
  friend size_t SnowflakeTest(size_t iters);

 public:
  Snowflake() = default;

  static void initWorkerId(meta::MetaClient* client);

  int64_t getId();

 private:
  /*
   *  Snowflake id: | timestampBit 41 | workerBit 10 | sequenceBit 12 |
   */
  std::atomic_int64_t lastTimestamp_{-1};  // 41 bits
  static inline int64_t workerId_{0};      // 10 bits
  std::atomic_int64_t sequence_{0};        // 12 bits

  int64_t getTimestamp();

  int64_t nextTimestamp();

  // start
  static constexpr int64_t startStmp_ = 1480166465631;
  static constexpr int64_t workerBit_ = 10;
  static constexpr int64_t sequenceBit_ = 12;

  static constexpr int64_t maxMachineId_ = (1 << workerBit_) - 1;
  static constexpr int64_t maxSequenceId_ = (1 << sequenceBit_) - 1;

  static constexpr int64_t machineLeft = sequenceBit_;
  static constexpr int64_t timestampLeft = sequenceBit_ + workerBit_;
};

}  // namespace nebula

#endif  // COMMON_ID_SNOWFLAKE_H_
