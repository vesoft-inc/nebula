/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/runtime/MemoryTracker.h"

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
