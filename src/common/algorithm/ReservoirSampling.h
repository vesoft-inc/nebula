/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_ALGORITHM_RESERVOIR_H_
#define COMMON_ALGORITHM_RESERVOIR_H_

#include "time/WallClock.h"
#include <random>

namespace nebula {
namespace algorithm {
template <class T>
class ReservoirSampling final {
public:
    explicit ReservoirSampling(int64_t num) {
        num_ = num;
        rng_.seed(nebula::time::WallClock::fastNowInMicroSec());
        samples_.reserve(num);
    }

    void sampling(T&& sample) {
        ++cnt_;
        if (cnt_ < num_) {
            samples_.emplace_back(std::move(sample));
        } else {
            int64_t index = std::uniform_int_distribution<>(0, cnt_)(rng_);
            if (index < num_) {
                samples_[index] = (std::move(sample));
            }
        }
    }

    std::vector<T>&& samples() {
        return std::move(samples_);
    }

private:
    std::vector<T>          samples_;
    std::mt19937_64         rng_;
    int64_t                 cnt_{-1};
    int64_t                 num_{-1};
};
}  // namespace algorithm
}  // namespace nebula
#endif
