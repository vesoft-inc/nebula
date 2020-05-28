/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/concurrent/Latch.h"

namespace nebula {
namespace concurrent {

Latch::Latch(size_t counter) {
    if (counter == 0) {
        throw std::invalid_argument("Zero Latch counter");
    }
    counter_ = counter;
}

void Latch::down() {
    std::unique_lock<std::mutex>  unique(lock_);
    if (counter_ == 0) {
        throw std::runtime_error("Count down on zero Latch");
    }
    if (--counter_ == 0) {
        cond_.notify_all();
    }
}

void Latch::downWait() {
    std::unique_lock<std::mutex> unique(lock_);
    if (counter_ == 0) {
        throw std::runtime_error("Count down on zero Latch");
    }
    if (--counter_ == 0) {
        cond_.notify_all();
        return;
    }
    cond_.wait(unique, [this] () { return counter_ == 0; });
}

void Latch::wait() {
    std::unique_lock<std::mutex> unique(lock_);
    cond_.wait(unique, [this] () { return counter_ == 0; });
}

bool Latch::isReady() {
    return counter_ == 0;
}

}   // namespace concurrent
}   // namespace nebula
