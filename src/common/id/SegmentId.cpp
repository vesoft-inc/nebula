/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "common/id/SegmentId.h"

namespace nebula {

int64_t SegmentId::getId() {
  if (cur_ < segmentStart_ + step_ - 1) {
    // prefetch next segment
    if (cur_ < segmentStart_ + (step_ / 2) - 1) asyncFetchSegment();
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

void SegmentId::asyncFetchSegment() {
  auto future = client_->getSegmentId(step_);
  // TODO
  std::move(future).via(runner_).then();
}

int64_t SegmentId::fetchSegment() {
  auto result = client_->getSegmentId(step_).get();
  if (result.ok()) {
    return result.value();
  } else {
    LOG(ERROR) << "Failed to fetch segment id from meta server";
    return -1;
  }
}
}  // namespace nebula
