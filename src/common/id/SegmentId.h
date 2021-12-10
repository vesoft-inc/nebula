/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"

namespace nebula {

// Segment auto-increase id
class SegmentId {
 public:
  explicit SegmentId(meta::MetaClient client, folly::Executor* runner)
      : client_(client), runner_(runner) {
    segmentStart_ = fetchSegment();
    cur_ = segmentStart_ - 1;
  }

  int64_t getId() {
    if (cur_ < segmentStart_ + step - 1) {
      // prefetch next segment
      if (cur_ < segmentStart_ + (step / 2) - 1) asyncFetchSegment();
      cur_ += 1;
    } else {  // cur == segment end
      if (segmentStart_ >= nextSegmentStart_) {
        // indicate asyncFetchSegment failed
        nextSegmentStart_ = fetchSegment();
      }
      segmentStart_ = nextSegmentStart_;
      cur_ = segmentStart_;
    }

    return cur_;
  }

 private:
  // when get id fast or fetchSegment() slow, we use all id in segment but nextSegmentStart_
  // isn't updated. In this case, we will getSegmentId() directly. In case this function update
  // after getSegmentId(), adding che here.
  void asyncFetchSegment() {
    auto future = client_.getSegmentId();
    std::move(future).via(runner_).then([this](auto& result) {
      if (result.ok()) {
        this->nextSegmentStart_ = result.value().get_segment_id();
      } else {
        LOG(ERROR) << "Failed to fetch segment id: " << result.status();
      }
    });
  }

  int64_t fetchSegment() {
    auto result = client_.getSegmentId().get();
    if (result.ok()) {
      return result.value().get_segment_id();
    } else {
      LOG(ERROR) << "Failed to fetch segment id from meta server";
      return -1;
    }
  }

  meta::MetaClient client_;
  folly::Executor* runner_;

  int64_t cur_;
  int64_t step;

  int64_t segmentStart_ = -1;
  int64_t nextSegmentStart_ = -1;
};

}  // namespace nebula
