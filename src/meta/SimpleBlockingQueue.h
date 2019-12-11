/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _NB_BLOCKINGlist_H_
#define _NB_BLOCKINGlist_H_

#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <list>
#include <assert.h>
#include <boost/optional.hpp>

#include <iostream>

template <typename T>
class SimpleBlockingQueue {
public:
    using MutexLockGuard = std::lock_guard<std::mutex>;

    SimpleBlockingQueue()
        : mutex_(),
          notEmpty_(),
          list_() {}

    SimpleBlockingQueue(const SimpleBlockingQueue &) = delete;
    SimpleBlockingQueue& operator=(const SimpleBlockingQueue &) = delete;

    void put(const T &x) {
        {
            MutexLockGuard lock(mutex_);
            list_.push_back(x);
        }
        notEmpty_.notify_one();
    }

    void put(T &&x) {
        {
            MutexLockGuard lock(mutex_);
            list_.push_back(std::move(x));
        }
        notEmpty_.notify_one();
    }

    bool remove(const T& x) {
        MutexLockGuard lock(mutex_);
        auto it = std::find(list_.begin(), list_.end(), x);
        if (it == list_.end()) {
            return false;
        }
        list_.erase(it);
        return true;
    }

    boost::optional<T> take() {
        std::unique_lock<std::mutex> lock(mutex_);
        notEmpty_.wait(lock, [this]{  return !this->list_.empty() || stopped_; });

        if (stopped_) return boost::optional<T>{};
        T ret(std::move(list_.front()));
        list_.pop_front();
        return ret;
    }

    size_t size() const {
        MutexLockGuard lock(mutex_);
        return list_.size();
    }

    void stop() {
        {
            MutexLockGuard lock(mutex_);
            stopped_ = true;
        }
        notEmpty_.notify_one();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable notEmpty_;
    std::list<T> list_;
    bool stopped_{false};
};

#endif  // _NB_BLOCKINGlist_H_
