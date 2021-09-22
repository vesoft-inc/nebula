/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "common/base/StatusOr.h"

namespace nebula {

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
};

}  // namespace nebula
