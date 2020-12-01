/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/base/ConcurrentLRUCache.h"
#include <gtest/gtest.h>

namespace nebula {

TEST(ConcurrentLRUCacheTest, SimpleTest) {
    ConcurrentLRUCache<int32_t, std::string> cache(1024);
    cache.insert(10, "ten");
    {
        auto v = cache.get(10);
        EXPECT_TRUE(v.ok());
        EXPECT_EQ("ten", v.value());
    }

    {
        auto v = cache.get(5);
        EXPECT_FALSE(v.ok());
    }

    EXPECT_EQ(5, cache.bucketIndex(100, 5));
    EXPECT_EQ(11, cache.bucketIndex(100, 11));
    EXPECT_EQ(0, cache.bucketIndex(100, 0));
    EXPECT_EQ(1024 % 16, cache.bucketIndex(100, 1024));
    EXPECT_EQ(std::hash<int>()(100) % 16, cache.bucketIndex(100, -1));

    EXPECT_EQ(0, cache.evicts());
    EXPECT_EQ(1, cache.hits());
    EXPECT_EQ(2, cache.total());

    for (auto i = 0; i < 100; i++) {
        auto v = cache.get(10);
        EXPECT_TRUE(v.ok());
        EXPECT_EQ("ten", v.value());
    }
    EXPECT_EQ(0, cache.evicts());
    EXPECT_EQ(101, cache.hits());
    EXPECT_EQ(102, cache.total());
}

TEST(ConcurrentLRUCacheTest, PutIfAbsentTest) {
    ConcurrentLRUCache<int32_t, std::string> cache(1024);
    {
        auto v = cache.putIfAbsent(10, "ten");
        EXPECT_EQ(Status::Inserted(), v.status());
    }

    {
        auto v = cache.putIfAbsent(10, "ele");
        EXPECT_TRUE(v.ok());
        EXPECT_EQ("ten", v.value());
    }

    EXPECT_EQ(0, cache.evicts());
    EXPECT_EQ(1, cache.hits());
    EXPECT_EQ(2, cache.total());
}

TEST(ConcurrentLRUCacheTest, EvictTest) {
    ConcurrentLRUCache<int32_t, std::string> cache(1000, 0);
    for (auto j = 0; j < 1000; j++) {
        cache.insert(j, folly::stringPrintf("%d_str", j));
    }
    for (auto i = 0; i < 1000; i++) {
        auto v = cache.get(i);
        EXPECT_TRUE(v.ok());
        EXPECT_EQ(folly::stringPrintf("%d_str", i), v.value());
    }
    for (auto j = 1000; j < 2000; j++) {
        cache.insert(j, folly::stringPrintf("%d_str", j));
    }
    for (auto i = 1000; i < 2000; i++) {
        auto v = cache.get(i);
        EXPECT_TRUE(v.ok());
        EXPECT_EQ(folly::stringPrintf("%d_str", i), v.value());
    }
    for (auto i = 0; i < 1000; i++) {
        auto v = cache.get(i);
        EXPECT_FALSE(v.ok());
    }
    EXPECT_EQ(1000, cache.evicts());
    EXPECT_EQ(2000, cache.hits());
    EXPECT_EQ(3000, cache.total());
}

TEST(ConcurrentLRUCacheTest, EvictKeyTest) {
    ConcurrentLRUCache<int32_t, std::string> cache(1024, 4);
    for (auto j = 0; j < 1000; j++) {
        cache.insert(j, folly::stringPrintf("%d_str", j));
    }
    for (auto i = 0; i < 1000; i++) {
        auto v = cache.get(i);
        EXPECT_TRUE(v.ok());
        EXPECT_EQ(folly::stringPrintf("%d_str", i), v.value());
    }
    for (auto i = 1; i < 1000; i+=2) {
        cache.evict(i);
    }
    for (auto i = 0; i < 1000; i++) {
        auto v = cache.get(i);
        if (i % 2 != 0) {
            EXPECT_FALSE(v.ok());
        } else {
            EXPECT_TRUE(v.ok());
        }
    }

    EXPECT_EQ(500, cache.evicts());
    EXPECT_EQ(1500, cache.hits());
    EXPECT_EQ(2000, cache.total());
}

TEST(ConcurrentLRUCacheTest, MultiThreadsTest) {
    ConcurrentLRUCache<int32_t, std::string> cache(1024 * 1024);
    std::vector<std::thread> threads;
    for (auto i = 0; i < 10; i++) {
        threads.emplace_back([&cache, i] () {
            for (auto j = i * 1000; j < (i + 1) *1000; j++) {
                cache.insert(j, folly::stringPrintf("%d_str", j));
            }
        });
    }
    for (auto i = 0; i < 10; i++) {
        threads[i].join();
    }
    for (auto i = 0; i < 10000; i++) {
        auto v = cache.get(i);
        EXPECT_TRUE(v.ok());
        EXPECT_EQ(folly::stringPrintf("%d_str", i), v.value());
    }

    EXPECT_EQ(0, cache.evicts());
    EXPECT_EQ(10000, cache.hits());
    EXPECT_EQ(10000, cache.total());
}

TEST(ConcurrentLRUCacheTest, OverwriteTest) {
    ConcurrentLRUCache<int32_t, std::string> cache(1024);
    cache.insert(10, "ten");
    {
        auto v = cache.get(10);
        EXPECT_TRUE(v.ok());
        EXPECT_EQ("ten", v.value());
    }
    cache.insert(10, "ten_v1");
    {
        auto v = cache.get(10);
        EXPECT_TRUE(v.ok());
        EXPECT_EQ("ten_v1", v.value());
    }
}

}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
