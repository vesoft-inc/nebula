/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"

namespace nebula {
namespace meta {

// Segment auto-increase id
class SegmentId {
 public:
  int64_t getId() {
    if (cur_ < segmentStart_ + (step >> 1) - 1) {
      cur_ += 1;
    } else if (cur_ < segmentStart_ + step - 1) {
      if (segmentStart_ == nextSegmentStart_) {
        fetchSegment();
      }
      cur_ += 1;
    } else {
      if (segmentStart_ >= nextSegmentStart_) {
        // handle
        // 这里要注意
        nextSegmentStart_ = client_.getSegmentId();
      }
      segmentStart_ = nextSegmentStart_;
      cur_ = segmentStart_;
    }

    return cur_;
  }

 private:
  // wip: async
  void asyncFetchSegment() {
    std::move auto result = client_.getSegmentId().get();
    // when get id fast or fetchSegment() slow, we use all id in segment but nextSegmentStart_
    // isn't updated. In this case, we will getSegmentId() directly. In case this function update
    // after getSegmentId(), adding che here.
    if (result.ok()) {
      if (segmentStart_ == nextSegmentStart_) {
        nextSegmentStart_ = result.value();
      }
    } else {
      LOG(ERROR) << "Failed to fetch segment id: " << result.status();
    }
  }

  meta::MetaClient client_;

  int64_t cur_;

  int64_t segmentStart_;
  int64_t nextSegmentStart_;

  int64_t step;
};

}  // namespace meta
}  // namespace nebula
