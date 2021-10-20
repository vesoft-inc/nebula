/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "common/memory/MemoryUtils.h"

DECLARE_bool(containerized);
DECLARE_double(system_memory_high_watermark_ratio);

namespace nebula {

TEST(MemoryHighWatermarkTest, TestHitsHighWatermarkInHost) {
  FLAGS_containerized = false;
  FLAGS_system_memory_high_watermark_ratio = 0.01;
  auto status = MemoryUtils::hitsHighWatermark();
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(std::move(status).value());
}

TEST(MemoryHighWatermarkTest, TestNotHitsHighWatermarkInHost) {
  FLAGS_containerized = false;
  FLAGS_system_memory_high_watermark_ratio = 0.99;
  auto status = MemoryUtils::hitsHighWatermark();
  ASSERT_TRUE(status.ok());
  ASSERT_FALSE(std::move(status).value());
}

TEST(MemoryHighWatermarkTest, DISABLED_TestHitsHighWatermarkInContainer) {
  FLAGS_containerized = true;
  FLAGS_system_memory_high_watermark_ratio = 0.01;
  auto status = MemoryUtils::hitsHighWatermark();
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(std::move(status).value());
}

TEST(MemoryHighWatermarkTest, TestNotHitsHighWatermarkInContainer) {
  FLAGS_containerized = true;
  FLAGS_system_memory_high_watermark_ratio = 0.99;
  auto status = MemoryUtils::hitsHighWatermark();
  ASSERT_TRUE(status.ok());
  ASSERT_FALSE(std::move(status).value());
}

}  // namespace nebula
