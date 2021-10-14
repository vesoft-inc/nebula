/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/ThreadLocal.h>

namespace nebula {

class MemoryTracker;

namespace thread {

struct ThreadState {
  MemoryTracker* memTracker;
};

extern folly::ThreadLocal<ThreadState> kCurrentThreadState;

}  // namespace thread
}  // namespace nebula
