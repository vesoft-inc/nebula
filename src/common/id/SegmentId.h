/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_ID_SEGMENTINCR_H_
#define COMMON_ID_SEGMENTINCR_H_

#include "clients/meta/MetaClient.h"

namespace nebula {
// Segment auto-increase id
class SegmentId {
 public:
  SegmentId(meta::BaseMetaClient* client, folly::Executor* runner)
      : client_(client), runner_(runner) {}

  ~SegmentId() = default;

  SegmentId(const SegmentId&) = delete;

  SegmentId& operator=(const SegmentId&) = delete;

  Status init(int64_t step);

  StatusOr<int64_t> getId();

 private:
  // when get id fast or fetchSegment() slow or fail, getSegmentId() directly.
  // In this case, the new segment will overlap with the old one.
  void asyncFetchSegment();

  StatusOr<int64_t> fetchSegment();

  std::mutex mutex_;

  int64_t cur_{-1};
  int64_t step_{-1};

  int64_t segmentStart_{-2};
  int64_t nextSegmentStart_{-2};

  // ensure the segment can be use for 10 mins.
  // 2 segment = max insert/secs * 600. segment = 400000 * 600 / 2 = 120000000
  static inline constexpr int64_t kMinStep_{120000000};

  meta::BaseMetaClient* client_;
  folly::Executor* runner_;
};
}  // namespace nebula

#endif  // COMMON_ID_SEGMENTINCR_H_
