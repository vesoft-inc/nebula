/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_ALGORITHM_RESERVOIR_H_
#define COMMON_ALGORITHM_RESERVOIR_H_

#include "common/base/Base.h"
#include "common/time/WallClock.h"

namespace nebula {
namespace algorithm {
template <class T>
class ReservoirSampling final {
 public:
  explicit ReservoirSampling(uint64_t num) {
    num_ = num;
    samples_.reserve(num);
  }

  explicit ReservoirSampling(uint64_t num, uint64_t count) {
    num_ = num;
    samples_.reserve(num);
    for (uint64_t i = 0; i < count; ++i) {
      sampling(i);
    }
  }

  bool sampling(T&& sample) {
    if (cnt_ < num_) {
      samples_.emplace_back(std::move(sample));
      ++cnt_;
      return true;
    } else {
      auto index = folly::Random::rand64(cnt_);
      if (index < num_) {
        samples_[index] = (std::move(sample));
        ++cnt_;
        return true;
      }
    }
    ++cnt_;
    return false;
  }

  std::vector<T> samples() {
    auto result = std::move(samples_);
    samples_.clear();
    samples_.reserve(num_);
    cnt_ = 0;
    return result;
  }

 private:
  std::vector<T> samples_;
  uint64_t cnt_{0};
  uint64_t num_{0};
};
}  // namespace algorithm
}  // namespace nebula
#endif
