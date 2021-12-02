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
        nextSegmentStart_ = client_.getSegment();
      }
      segmentStart_ = nextSegmentStart_;
      cur_ = segmentStart_;
    }

    return cur_;
  }

 private:
  // wip: async
  void fetchSegment() {
    int64_t newSegment = client_.getSegment();
    // when get id fast or fetchSegment() slow, we use all id in segment but nextSegmentStart_
    // isn't updated. In this case, we will getSegment() directly. In case this function update
    // after getSegment(), adding che here.
    if (segmentStart_ == nextSegmentStart_) {
      nextSegmentStart_ = newSegment;
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
