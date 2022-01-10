/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_ID_SEGMENTINCR_H_
#define COMMON_ID_SEGMENTINCR_H_

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"

namespace nebula {
// Segment auto-increase id
class SegmentId {
  FRIEND_TEST(SegmentIdTest, TestConcurrency);

 public:
  static SegmentId& getInstance() {
    ASSERT(client_ != nullptr);
    ASSERT(runner_ != nullptr);
    static SegmentId instance;
    return instance;
  }

  ~SegmentId() = default;

  SegmentId(const SegmentId&) = delete;

  SegmentId& operator=(const SegmentId&) = delete;

  Status init(int64_t step) {
    step_ = step;
    if (step < 10000) {
      return Status::Error("Step is too small");
    }

    auto xRet = fetchSegment();
    NG_RETURN_IF_ERROR(xRet);

    segmentStart_ = xRet.value();
    cur_ = segmentStart_ - 1;

    return Status::OK();
  }

  static void initClient(meta::BaseMetaClient* client) {
    client_ = client;
  }

  static void initRunner(folly::Executor* runner) {
    runner_ = runner;
  }

  StatusOr<int64_t> getId();

 private:
  SegmentId() = default;

  // when get id fast or fetchSegment() slow, we use all id in segment but nextSegmentStart_
  // isn't updated. In this case, we will getSegmentId() directly. In case this function update
  // after getSegmentId(), adding che here.
  void asyncFetchSegment();

  StatusOr<int64_t> fetchSegment();

  std::mutex mutex_;

  int64_t cur_{-1};
  int64_t step_{-1};

  int64_t segmentStart_{-1};
  int64_t nextSegmentStart_{-1};

  static inline meta::BaseMetaClient* client_{nullptr};
  static inline folly::Executor* runner_{nullptr};
};
}  // namespace nebula

#endif  // COMMON_ID_SEGMENTINCR_H_
