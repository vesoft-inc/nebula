/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once


#include <algorithm>
#include "utils/MemoryLockCore.h"

namespace nebula {

// RAII style to easily control the lock acquire / release
template<class Key>
class MemoryLockGuard {
public:
    MemoryLockGuard(MemoryLockCore<Key>* lock, const Key& key)
        : MemoryLockGuard(lock, std::vector<Key>{key}) {}

    MemoryLockGuard(MemoryLockCore<Key>* lock,
                    const std::vector<Key>& keys,
                    bool dedup = false,
                    bool prepCheck = true)
        : lock_(lock), keys_(keys) {
        if (dedup) {
            std::sort(keys_.begin(), keys_.end());
            keys_.erase(unique(keys_.begin(), keys_.end()), keys_.end());
        }
        if (prepCheck) {
            std::tie(iter_, locked_) = lock_->lockBatch(keys_);
        } else {
            locked_ = true;
        }
    }

    MemoryLockGuard(const MemoryLockGuard&) = delete;

    MemoryLockGuard(MemoryLockGuard&& lg) noexcept
        : lock_(lg.lock_), keys_(std::move(lg.keys_)), locked_(lg.locked_) {}

    MemoryLockGuard& operator=(const MemoryLockGuard&) = delete;

    MemoryLockGuard& operator=(MemoryLockGuard&& lg) noexcept {
        if (this != &lg) {
            lock_ = lg.lock_;
            keys_ = std::move(lg.keys_);
            locked_ = lg.locked_;
        }
        return *this;
    }

    ~MemoryLockGuard() {
        if (locked_) {
            lock_->unlockBatch(keys_);
        }
    }

    bool isLocked() const noexcept {
        return locked_;
    }

    operator bool() const noexcept {
        return isLocked();
    }

    // return the first conflict key, if any
    // this has to be called iff locked_ is false;
    Key conflictKey() {
        CHECK(locked_ == false);
        return *iter_;
    }

protected:
    MemoryLockCore<Key>* lock_;
    std::vector<Key> keys_;
    typename std::vector<Key>::iterator iter_;
    bool locked_{false};
};

}  // namespace nebula
