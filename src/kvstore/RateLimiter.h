/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_RATELIMITER_H
#define KVSTORE_RATELIMITER_H

#include <folly/TokenBucket.h>

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "common/time/WallClock.h"

DECLARE_bool(skip_wait_in_rate_limiter);

namespace nebula {
namespace kvstore {

// A simple wrapper for folly::TokenBucket, it would limit the speed to rate_ * buckets_.size().
// For now, there are two major cases: snapshot (both for balance or catch up) and rebuild index.
class RateLimiter {
 public:
  RateLimiter() {
    // token will be available after 1 second, to prevent speed spike at the beginning
    auto now = time::WallClock::fastNowInSec();
    int64_t waitInSec = FLAGS_skip_wait_in_rate_limiter ? 0 : 1;
    bucket_.reset(new folly::DynamicTokenBucket(static_cast<double>(now + waitInSec)));
  }

  /**
   * @brief Consume some budget from rate limiter. Caller must make sure the **the partition has
   * been add, and won't be removed during consume.** Snapshot and rebuild index follow this
   * principle by design.
   *
   * @param toConsume Amount to consume
   * @param rate Generate speed
   * @param burstSize Maximum consume speed to consume
   */
  void consume(double toConsume, double rate, double burstSize) {
    if (toConsume > burstSize) {
      // consumeWithBorrowAndWait do nothing when toConsume > burstSize_, we sleep 1s instead
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      // If there are enough tokens, consume and return immediately.
      // If not, consume anyway, but sleep enough time before return.
      auto now = time::WallClock::fastNowInSec();
      bucket_->consumeWithBorrowAndWait(toConsume, rate, burstSize, static_cast<double>(now));
    }
  }

 private:
  std::unique_ptr<folly::DynamicTokenBucket> bucket_;
};

}  // namespace kvstore
}  // namespace nebula
#endif
