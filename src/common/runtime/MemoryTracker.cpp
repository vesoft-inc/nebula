/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/runtime/MemoryTracker.h"

#include <limits>

// TODO(yee): It's better to set the configuration in session level. After we refactored our
// configuration implementation, should come back to delete it.
DEFINE_int64(memory_limit_bytes_per_query,
             std::numeric_limits<int64_t>::max(),
             "Memory limit bytes usage per query.");

namespace nebula {

MemoryTracker::MemoryTracker(int64_t limit) : usage_(0), limit_(limit) {}

bool MemoryTracker::consume(int64_t bytes) {
  usage_ += bytes;
  if (usage_ > limit_) {
    // TODO(yee): Try more actions
    return false;
  }
  return true;
}

}  // namespace nebula
