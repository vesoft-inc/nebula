/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 */

#include <folly/concurrency/ConcurrentHashMap.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <thread>

#include "common/id/Snowflake.h"

namespace nebula {

int64_t getSequence(int64_t workerId) {
  return workerId & 0xfff;
}
int64_t getWorkerId(int64_t id) {
  return (id >> 12) & 0x3ff;
}
int64_t getTimestamp(int64_t id) {
  return id >> 22;
}

TEST(SnowflakeTest, TestWorkerId) {
  int64_t workerId = 0;

  Snowflake generator;
  Snowflake::workerId_ = 0;
  int64_t id = generator.getId();

  ASSERT_EQ(getSequence(id), workerId);
}

void proc(Snowflake& generator, folly::ConcurrentHashMap<int64_t, int>& map) {
  int times = 10000;

  for (int i = 0; i < times; i++) {
    int64_t id = generator.getId();
    ASSERT_FALSE(map.find(id) == map.end());
    map.insert(id, 0);
  }
}

TEST(SnowflakeTest, TestConcurrency) {
  Snowflake::workerId_ = 0;
  int threadNum = 32;

  Snowflake generator;
  folly::ConcurrentHashMap<int64_t, int> map;
  std::vector<std::thread> threads;
  threads.reserve(threadNum);

  for (int i = 0; i < threadNum; i++) {
    threads.emplace_back(std::thread(proc, std::ref(generator), std::ref(map)));
  }

  for (int i = 0; i < threadNum; i++) {
    threads[i].join();
  }
}

}  // namespace nebula
