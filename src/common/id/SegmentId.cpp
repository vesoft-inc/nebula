/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/id/SegmentId.h"

namespace nebula {

StatusOr<int64_t> SegmentId::getId() {
  std::lock_guard<std::mutex> guard(mutex_);

  if (cur_ < segmentStart_ + step_ - 1) {
    // non-block prefetch next segment
    if (cur_ == segmentStart_ + (step_ / 2) - 1) {
      asyncFetchSegment();
    }
    cur_ += 1;
  } else {  // cur == segment end
    if (segmentStart_ >= nextSegmentStart_) {
      // indicate asyncFetchSegment() failed or fetchSegment() slow
      LOG(ERROR)
          << "segmentId asyncFetchSegment() failed or slow(step is too small), segmentStart_: "
          << segmentStart_ << ", nextSegmentStart_: " << nextSegmentStart_;
      auto xRet = fetchSegment();
      NG_RETURN_IF_ERROR(xRet);
      nextSegmentStart_ = xRet.value();
    }
    segmentStart_ = nextSegmentStart_;
    cur_ = segmentStart_;
  }

  return cur_;
}

void SegmentId::asyncFetchSegment() {
  auto future = client_->getSegmentId(step_);
  std::move(future).via(runner_).thenValue([this](StatusOr<int64_t> resp) {
    NG_RETURN_IF_ERROR(resp);
    if (!resp.value()) {
      return Status::Error("asyncFetchSegment failed!");
    }
    this->nextSegmentStart_ = resp.value();
    return Status::OK();
  });
}

StatusOr<int64_t> SegmentId::fetchSegment() {
  auto result = client_->getSegmentId(step_).get();

  NG_RETURN_IF_ERROR(result);
  return result.value();
}

Status SegmentId::init(int64_t step) {
  step_ = step;
  if (step < kMinStep_) {
    return Status::Error("Step is too small");
  }

  auto xRet = fetchSegment();
  NG_RETURN_IF_ERROR(xRet);

  segmentStart_ = xRet.value();
  cur_ = segmentStart_ - 1;

  return Status::OK();
}

}  // namespace nebula
