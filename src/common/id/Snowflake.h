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
  friend int64_t getSequence(int64_t id);
  friend int64_t getWorkerId(int64_t id);
  friend int64_t getTimestamp(int64_t id);

 public:
  Snowflake() = default;

  static void initWorkerId(meta::MetaClient* client);

  int64_t getId();

 private:
  /*
   *  Snowflake id:
   *  timestampBit 38 bits (8.6 years) |
   *  workerBit 13 bits (8k workerid) |
   *  sequenceBit 12 bits (4 million per second) |
   */
  int64_t lastTimestamp_{-1};
  static inline int64_t workerId_{0};
  int64_t sequence_{0};

  static int64_t getTimestamp();

  int64_t nextTimestamp();
  int64_t getIdByTs(int64_t timestamp);

  std::mutex mutex_;

  // start
  static constexpr int64_t startStmp = 1577808000000;  // 2020-01-01 00:00:00
  static constexpr int64_t workerBit = 12;
  static constexpr int64_t sequenceBit = 14;

  static constexpr int64_t maxWorkerId = (1 << workerBit) - 1;
  static constexpr int64_t maxSequence = (1 << sequenceBit) - 1;

  static constexpr int64_t workerLeft = sequenceBit;
  static constexpr int64_t timestampLeft = sequenceBit + workerBit;

  static constexpr int64_t firstBitRevert = 0x9000000000000000;
};

}  // namespace nebula

#endif  // COMMON_ID_SNOWFLAKE_H_
