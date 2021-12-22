/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <cstdint>

#include "common/id/Snowflake.h"

namespace nebula {

int64_t getSequence(int64_t workerId) { return workerId & 0xfff; }
int64_t getWorkerId(int64_t id) { return (id >> 12) & 0x3ff; }
int64_t getTimestamp(int64_t id) { return id >> 22; }

TEST(SnowflakeTest, TestWorkerId) {
  int64_t workerId = 0;

  Snowflake generator;
  generator.mockInitWorkerId(workerId);
  int64_t id = generator.getId();

  ASSERT_EQ(getSequence(id), workerId);
}
}  // namespace nebula
