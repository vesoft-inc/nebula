/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_CONCURRENT_LATCH_H_
#define COMMON_CONCURRENT_LATCH_H_

#include "common/base/Base.h"
#include "common/cpp/helpers.h"
/**
 * Latch is an one-shot synchronization object.
 * It provides an synchronization point for multiple threads.
 * See shared/concurrent/test/LatchTest.cpp for use scenarios.
 */

namespace nebula {
namespace concurrent {

class Latch final : public nebula::cpp::NonCopyable, public nebula::cpp::NonMovable {
public:
    /**
     * @counter:  initial counter,
     *            typically number of participating threads.
     * Throws `std::invalid_argument' if counter is zero.
     */
    explicit Latch(size_t counter);
    ~Latch() = default;
    /**
     * Decrements the internal counter by one.
     * If the counter reaches 0, all blocking(in `wait')
     * threads will be given the green light.
     * Throws `std::runtime_error' if counter already zeroed.
     */
    void down();
    /**
     * Decrements the internal counter by one.
     * If the counter reaches 0, returns immediately,
     * and all blocking threads will be woken up.
     * Otherwise, the calling thread blocks until being woken up.
     * Throws `std::runtime_error' if counter already zeroed.
     */
    void downWait();
    /**
     * Blocks if internal counter not zero.
     * Otherwise, returns immediately.
     */
    void wait();
    /**
     * Returns true if internal counter already zeroed.
     * Otherwise, returns false.
     */
    bool isReady();

private:
    volatile size_t         counter_{0};
    std::mutex              lock_;
    std::condition_variable cond_;
};

}   // namespace concurrent
}   // namespace nebula

#endif  // COMMON_CONCURRENT_LATCH_H_
