/* Copyright (c) 2019 vesoft inc. All rights reserved.
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
    if (cur_ < segment_start_ + (step >> 1) - 1) {
      cur_ += 1;
    } else if (cur_ < segment_start_ + step - 1) {
      if (segment_start_ == next_segment_start_) {
        fetchSegment();
      }
      cur_ += 1;
    } else {
      if (segment_start_ >= next_segment_start_) {
        // handle
        // 这里要注意
        next_segment_start_ = client_.getSegment();
      }
      segment_start_ = next_segment_start_;
      cur_ = segment_start_;
    }

    return cur_;
  }

 private:
  // wip: async
  void fetchSegment() {
    int64_t new_segment = client_.getSegment();
    // when get id fast or fetchSegment() slow, we use all id in segment but next_segment_start_
    // isn't updated. In this case, we will getSegment() directly. In case this function update
    // after getSegment(), adding che here.
    if (segment_start_ == next_segment_start_) {
      next_segment_start_ = new_segment;
    }
  }

  meta::MetaClient client_;

  int64_t cur_;

  int64_t segment_start_;
  int64_t next_segment_start_;

  int64_t step;
};

}  // namespace meta
}  // namespace nebula
