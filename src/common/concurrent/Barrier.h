/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_CONCURRENT_BARRIER_H_
#define COMMON_CONCURRENT_BARRIER_H_

#include "common/base/Base.h"
#include "common/cpp/helpers.h"

/**
 * Like `Latch', `Barrier' is a synchronization object, except that
 * `Barrier' is reusable.
 * Besides, `Barrier' features with an optional callable object,
 * which would be invoked at the completion phase, i.e. synchronization point,
 * by the last participating thread who entered `wait', before waking up other blocking threads.
 */

namespace nebula {
namespace concurrent {

class Barrier final : public nebula::cpp::NonCopyable, public nebula::cpp::NonMovable {
public:
    /**
     * @counter     number of participating threads
     * @completion  callback invoked at the completion phase
     */
    explicit Barrier(size_t counter, std::function<void()> completion = nullptr);
    ~Barrier() = default;
    /**
     * Decrements the internal counter.
     * If the counter reaches zero, the completion callback would be invoked if present,
     * then all preceding blocked threads would be woken up, with the internal counter
     * reset to the original value.
     * Otherwise, the calling thread would be blocked to wait for
     * other participants' arrival.
     */
    void wait();

private:
    std::function<void()>   completion_{nullptr};
    size_t                  counter_{0};
    size_t                  ages_{0};
    size_t                  generation_{0};
    std::mutex              lock_;
    std::condition_variable cond_;
};

}   // namespace concurrent
}   // namespace nebula

#endif  // COMMON_CONCURRENT_BARRIER_H_
