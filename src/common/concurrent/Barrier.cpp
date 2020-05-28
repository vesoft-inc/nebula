/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/concurrent/Barrier.h"


namespace nebula {
namespace concurrent {

Barrier::Barrier(size_t counter, std::function<void()> completion) {
    if (counter == 0) {
        throw std::invalid_argument("Barrier counter can't be zero");
    }
    completion_ = std::move(completion);
    counter_ = counter;
    ages_ = counter_;
}

void Barrier::wait() {
    std::unique_lock<std::mutex> guard(lock_);
    if (--ages_ == 0) {
        ages_ = counter_;
        ++generation_;
        if (completion_ != nullptr) {
            completion_();
        }
        cond_.notify_all();
    } else {
        auto current = generation_;
        cond_.wait(guard, [=] () { return current != generation_; });
    }
}

}   // namespace concurrent
}   // namespace nebula
