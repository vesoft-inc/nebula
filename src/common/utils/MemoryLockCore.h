/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include <folly/concurrency/ConcurrentHashMap.h>

namespace nebula {

template<typename Key>
class MemoryLockCore {
public:
    MemoryLockCore() = default;

    ~MemoryLockCore() = default;

    // I assume this may have better perf in high contention
    // but may not necessary sort any time.
    // this may be useful while first lock attempt failed,
    // and try to retry.
    auto try_lockSortedBatch(const std::vector<Key>& keys) {
        return lockBatch(keys);
    }

    bool try_lock(const Key& key) {
        return hashMap_.insert(std::make_pair(key, 0)).second;
    }

    void unlock(const Key& key) {
        hashMap_.erase(key);
    }

    template<class Iter>
    std::pair<Iter, bool> lockBatch(Iter begin, Iter end) {
        Iter curr = begin;
        bool inserted = false;
        while (curr != end) {
            std::tie(std::ignore, inserted) = hashMap_.insert(std::make_pair(*curr, 0));
            if (!inserted) {
                unlockBatch(begin, curr);
                return std::make_pair(curr, false);
            }
            ++curr;
        }
        return std::make_pair(end, true);
    }

    template<class Collection>
    auto lockBatch(Collection&& collection) {
        return lockBatch(collection.begin(), collection.end());
    }

    template<class Iter>
    void unlockBatch(Iter begin, Iter end) {
        for (; begin != end; ++begin) {
            hashMap_.erase(*begin);
        }
    }

    template<class Collection>
    auto unlockBatch(Collection&& collection) {
        return unlockBatch(collection.begin(), collection.end());
    }

    void clear() {
        hashMap_.clear();
    }

    size_t size() {
        return hashMap_.size();
    }

protected:
    folly::ConcurrentHashMap<Key, int> hashMap_;
};

}  // namespace nebula
