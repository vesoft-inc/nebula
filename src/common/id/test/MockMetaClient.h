/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/id/SegmentId.h"

namespace nebula {
class MockMetaClient : public meta::BaseMetaClient {
 public:
  folly::Future<StatusOr<int64_t>> getSegmentId(int64_t length) override {
    std::lock_guard<std::mutex> guard(mutex_);
    auto future = folly::makeFuture(cur_.load());
    cur_.fetch_add(length);
    return future;
  }

 private:
  std::mutex mutex_;
  std::atomic_int64_t cur_{0};
};
}  // namespace nebula
