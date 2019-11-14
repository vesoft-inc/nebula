/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_CONCURRENTLRUCACHE_H_
#define COMMON_BASE_CONCURRENTLRUCACHE_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include <boost/compute/detail/lru_cache.hpp>
#include <gtest/gtest_prod.h>

namespace nebula {

template<typename K, typename V>
class ConcurrentLRUCache final {
    FRIEND_TEST(ConcurrentLRUCacheTest, SimpleTest);

public:
    explicit ConcurrentLRUCache(size_t capacity, uint32_t buckets = 16)
        : bucketsNum_(buckets) {
        CHECK(capacity > buckets && buckets > 0);
        auto capPerBucket = capacity / buckets;
        auto left = capacity;
        for (uint32_t i = 0; i < buckets - 1; i++) {
            buckets_.emplace_back(capPerBucket);
            left -= capPerBucket;
        }
        CHECK_GT(left, 0);
        buckets_.emplace_back(left);
    }

    bool contains(const K& key, int32_t hint = -1) {
        return buckets_[bucketIndex(key, hint)].contains(key);
    }

    void insert(const K& key, const V& val, int32_t hint = -1) {
        buckets_[bucketIndex(key, hint)].insert(key, val);
    }

    StatusOr<V> get(const K& key, int32_t hint = -1) {
        return buckets_[bucketIndex(key, hint)].get(key);
    }

    void clear() {
        for (auto i = 0; i < bucketsNum_; i++) {
            buckets_[i].clear();
        }
    }

private:
    class Bucket {
    public:
        explicit Bucket(size_t capacity)
            : lru_(std::make_unique<boost::compute::detail::lru_cache<K, V>>(capacity)) {}

        Bucket(Bucket&& b)
            : lru_(std::move(b.lru_)) {}

        bool contains(const K& key) {
            std::lock_guard<std::mutex> guard(lock_);
            return lru_->contains(key);
        }

        void insert(const K& key, const V& val) {
            std::lock_guard<std::mutex> guard(lock_);
            lru_->insert(key, val);
        }

        StatusOr<V> get(const K& key) {
            std::lock_guard<std::mutex> guard(lock_);
            auto v = lru_->get(key);
            if (v == boost::none) {
                return Status::Error();
            }
            return std::move(v).value();
        }

        void clear() {
            std::lock_guard<std::mutex> guard(lock_);
            lru_->clear();
        }

    private:
        std::mutex lock_;
        std::unique_ptr<boost::compute::detail::lru_cache<K, V>> lru_;
    };


private:
    /**
     * If hint is specified, we could use it to cal the bucket index directly without hash key.
     * */
    uint32_t bucketIndex(const K& key, int32_t hint = -1) {
        return hint >= 0 ? hint % bucketsNum_ : std::hash<K>()(key) % bucketsNum_;
    }


private:
    std::vector<Bucket> buckets_;
    uint32_t bucketsNum_ = 1;
};

}  // namespace nebula

#endif  // COMMON_BASE_CONCURRENTLRUCACHE_H_

