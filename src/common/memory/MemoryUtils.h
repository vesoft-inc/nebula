/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_MEMORY_MEMORYUTILS_H
#define COMMON_MEMORY_MEMORYUTILS_H

#include <atomic>
#include <cstdint>
#include <string>

#include "common/base/StatusOr.h"

namespace nebula {
namespace memory {

/**
 * MemoryUtils will compute the memory consumption of containerization and physical machine
 * deployment:
 *  - physical machine: read the `/proc/meminfo'
 *  - container: read the `/sys/fs/memory/memory.{stat,usage_in_bytes,limit_in_bytes}'
 */
class MemoryUtils final {
 public:
  static StatusOr<bool> hitsHighWatermark();

  static std::atomic_bool kHitMemoryHighWatermark;

 private:
  MemoryUtils(const MemoryUtils &) = delete;
  void operator=(const MemoryUtils &) = delete;

  static StatusOr<uint64_t> readSysContents(const std::string &path);

  // Handle memory tracker related logic by inform system's total memory and current available
  // memory in bytes
  static void handleMemoryTracker(int64_t total, int64_t available);

 private:
  static int64_t kLastPurge_;
  static int64_t kLastPrintMemoryTrackerStats_;
  static int64_t kCurrentTotal_;
  static double kCurrentLimitRatio_;
};

}  // namespace memory
}  // namespace nebula
#endif
