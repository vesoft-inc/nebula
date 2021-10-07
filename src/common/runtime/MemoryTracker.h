/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <limits>

namespace nebula {

class MemoryTracker final {
 public:
  explicit MemoryTracker(int64_t limit = std::numeric_limits<int64_t>::max());

  bool consume(int64_t bytes);
  void release(int64_t bytes) { consume(-bytes); }

  int64_t usage() const { return usage_; }
  int64_t limit() const { return limit_; }

 private:
  std::atomic<int64_t> usage_{0};
  int64_t limit_;
};

}  // namespace nebula
