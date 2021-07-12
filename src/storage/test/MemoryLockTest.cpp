/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "utils/MemoryLockWrapper.h"

namespace nebula {
namespace storage {

using LockGuard = nebula::MemoryLockGuard<std::string>;

class MemoryLockTest : public ::testing::Test {
protected:
};

TEST_F(MemoryLockTest, SimpleTest) {
    MemoryLockCore<std::string> mlock;
    {
        LockGuard lk1(&mlock, "1");
        EXPECT_TRUE(lk1);

        LockGuard lk2(&mlock, "1");
        EXPECT_FALSE(lk2);
        LOG(INFO) << "conflict key = " << lk2.conflictKey();
    }
    {
        auto* lk1 = new LockGuard(&mlock, "1");
        EXPECT_TRUE(*lk1);

        std::vector<std::string> keys{"1", "2", "3"};
        LockGuard lk2(&mlock, keys);
        EXPECT_FALSE(lk2);
        LOG(INFO) << "conflict key = " << lk2.conflictKey();

        delete lk1;
        LockGuard lk3(&mlock, keys);
        EXPECT_TRUE(lk3);
    }
    {
        // if keys has dup, but not call the dedup ctor, may lock failed
        std::vector<std::string> keys{"1", "1", "1"};
        LockGuard lk1(&mlock, keys);
        EXPECT_FALSE(lk1);
        LOG(INFO) << "conflict key = " << lk1.conflictKey();

        LockGuard lk2(&mlock, keys, true);
        EXPECT_TRUE(lk2);
    }
}

TEST_F(MemoryLockTest, MoveTest) {
    MemoryLockCore<std::string> mlock;
    {
        LockGuard *lk1 = new LockGuard(&mlock, "1");
        EXPECT_TRUE(*lk1);

        auto lk2 = std::move(*lk1);
        EXPECT_TRUE(lk2);

        delete lk1;

        LockGuard lk3(&mlock, "1");
        EXPECT_FALSE(lk3);
    }
}

TEST_F(MemoryLockTest, PrepTest) {
    MemoryLockCore<std::string> mlock;
    {
        EXPECT_TRUE(mlock.try_lock("1"));
        EXPECT_TRUE(mlock.try_lock("2"));
        EXPECT_FALSE(mlock.try_lock("1"));
        EXPECT_FALSE(mlock.try_lock("2"));
        std::vector<std::string> keys{"1", "2"};
        auto* lk = new LockGuard(&mlock, keys, false, false);
        EXPECT_TRUE(lk);
        delete lk;
    }
    EXPECT_EQ(0, mlock.size());
}

TEST_F(MemoryLockTest, DedupTest) {
    MemoryLockCore<std::string> mlock;
    {
        std::vector<std::string> keys{"1", "2", "1", "2"};
        auto* lk = new LockGuard(&mlock, keys, true, false);
        EXPECT_TRUE(lk);
        EXPECT_EQ(0, mlock.size());
        delete lk;
    }
    EXPECT_EQ(0, mlock.size());
    {
        EXPECT_TRUE(mlock.try_lock("1"));
        EXPECT_TRUE(mlock.try_lock("2"));
        EXPECT_FALSE(mlock.try_lock("1"));
        EXPECT_FALSE(mlock.try_lock("2"));
        std::vector<std::string> keys{"1", "2", "1", "2"};
        auto* lk = new LockGuard(&mlock, keys, true, false);
        EXPECT_TRUE(lk);
        EXPECT_EQ(2, mlock.size());
        delete lk;
    }
    EXPECT_EQ(0, mlock.size());
    {
        std::vector<std::string> keys{"1", "2", "1", "2"};
        auto* lk = new LockGuard(&mlock, keys, true, true);
        EXPECT_TRUE(lk);
        EXPECT_EQ(2, mlock.size());
        delete lk;
    }
    EXPECT_EQ(0, mlock.size());
    {
        std::vector<std::string> keys{"1", "2", "1", "2"};
        LockGuard lk(&mlock, keys, false, true);
        EXPECT_FALSE(lk);
        EXPECT_EQ(0, mlock.size());
    }
    EXPECT_EQ(0, mlock.size());
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
