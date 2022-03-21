/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/concurrency/ConcurrentHashMap.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "common/id/Snowflake.h"

namespace nebula {

int64_t getSequence(int64_t id) {
  return id & Snowflake::kMaxSequence;
}
int64_t getWorkerId(int64_t id) {
  return (id >> Snowflake::kSequenceBit) & Snowflake::kMaxWorkerId;
}
int64_t getTimestamp(int64_t id) {
  return id >> (Snowflake::kSequenceBit + Snowflake::kWorkerBit);
}

TEST(SnowflakeTest, TestWorkerId) {
  int64_t maxWorkerId = 1023;

  Snowflake generator;

  for (int i = 0; i < maxWorkerId; i++) {
    Snowflake::workerId_ = i;
    int64_t id = generator.getId();

    ASSERT_EQ(getWorkerId(id), i);
  }
}

TEST(SnowflakeTest, TestConcurrency) {
  Snowflake::workerId_ = 0;
  int threadNum = 16;
  int times = 1000;

  Snowflake generator;
  folly::ConcurrentHashMap<int64_t, int> map;
  std::vector<std::thread> threads;
  threads.reserve(threadNum);

  auto proc = [&]() {
    for (int i = 0; i < times; i++) {
      int64_t id = generator.getId();
      ASSERT_TRUE(map.find(id) == map.end()) << "id: " << id;
      map.insert(id, 0);
    }
  };

  for (int i = 0; i < threadNum; i++) {
    threads.emplace_back(std::thread(proc));
  }

  for (int i = 0; i < threadNum; i++) {
    threads[i].join();
  }
}

}  // namespace nebula
