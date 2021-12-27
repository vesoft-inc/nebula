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
 public:
  static SegmentId& getInstance(int64_t step) {
    static SegmentId instance(step);
    return instance;
  }

  SegmentId(const SegmentId&) = delete;
  SegmentId& operator=(const SegmentId&) = delete;

  static void initClientAndRunner(meta::MetaClient* client, folly::Executor* runner) {
    client_ = client;
    runner_ = runner;
  }

  int64_t getId();

 private:
  explicit SegmentId(int64_t step) : step_(step) {
    segmentStart_ = fetchSegment();
    cur_ = segmentStart_ - 1;
  }
  ~SegmentId() = default;
  // when get id fast or fetchSegment() slow, we use all id in segment but nextSegmentStart_
  // isn't updated. In this case, we will getSegmentId() directly. In case this function update
  // after getSegmentId(), adding che here.
  void asyncFetchSegment();

  int64_t fetchSegment();

  static inline meta::MetaClient* client_ = nullptr;
  static inline folly::Executor* runner_ = nullptr;

  int64_t cur_ = -1;
  int64_t step_ = -1;

  int64_t segmentStart_ = -1;
  int64_t nextSegmentStart_ = -1;
};
}  // namespace nebula

#endif  // COMMON_ID_SEGMENTINCR_H_
