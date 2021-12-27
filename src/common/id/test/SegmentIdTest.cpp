/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "common/id/SegmentId.h"

namespace nebula {
TEST(SegmentIdTest, TestWorkerId) {
  int64_t workerId = 0;

  Snowflake generator;
  generator.mockInitWorkerId(workerId);
  int64_t id = generator.getId();

  ASSERT_EQ(getSequence(id), workerId);
}
}  // namespace nebula
