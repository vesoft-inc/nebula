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
  friend int64_t getSequence(int64_t id);
  friend int64_t getWorkerId(int64_t id);
  friend int64_t getTimestamp(int64_t id);

 public:
  Snowflake() = default;

  static bool initWorkerId(meta::MetaClient* client);

  int64_t getId();

 private:
  static int64_t getTimestamp();

  int64_t nextTimestamp();

  int64_t getIdByTs(int64_t timestamp);

  std::mutex mutex_;
  /*
   *  Snowflake id:
   *  timestampBit 38 bits (8.6 years) |
   *  workerBit 13 bits (8k workerid) |
   *  sequenceBit 12 bits (4 million per second) |
   */
  int64_t lastTimestamp_{-1};
  static inline int64_t workerId_{0};
  int64_t sequence_{0};

  static constexpr int64_t kStartStmp = 1577808000000;  // start time: 2020-01-01 00:00:00
  static constexpr int64_t kWorkerBit = 13;             // worker id bit
  static constexpr int64_t kSequenceBit = 12;           // sequence bit

  static constexpr int64_t kMaxWorkerId = (1 << kWorkerBit) - 1;  // as worker id mask
  static constexpr int64_t kMaxSequence = (1 << kSequenceBit) - 1;

  static constexpr int64_t kWorkerIdLeft = kSequenceBit;  // workerId left bits
  static constexpr int64_t kTimestampLeft = kSequenceBit + kWorkerBit;

  static constexpr int64_t kFirstBitRevert = 0x9000000000000000;  // revert the first bit
};

}  // namespace nebula

#endif  // COMMON_ID_SNOWFLAKE_H_
