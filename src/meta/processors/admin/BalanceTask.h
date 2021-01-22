/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADMIN_BALANCETASK_H_
#define META_ADMIN_BALANCETASK_H_

#include <gtest/gtest_prod.h>
#include "meta/ActiveHostsMan.h"
#include "common/time/WallClock.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/Common.h"
namespace nebula {
namespace meta {
class BalanceTask {
    friend class BalancePlan;
    FRIEND_TEST(BalanceTest, BalanceTaskTest);
    FRIEND_TEST(BalanceTest, BalancePlanTest);
    FRIEND_TEST(BalanceTest, SpecifyHostTest);
    FRIEND_TEST(BalanceTest, SpecifyMultiHostTest);
    FRIEND_TEST(BalanceTest, MockReplaceMachineTest);
    FRIEND_TEST(BalanceTest, SingleReplicaTest);
    FRIEND_TEST(BalanceTest, NormalTest);
    FRIEND_TEST(BalanceTest, TryToRecoveryTest);
    FRIEND_TEST(BalanceTest, RecoveryTest);
    FRIEND_TEST(BalanceTest, StopPlanTest);

public:
    BalanceTask() = default;

    BalanceTask(BalanceID balanceId,
                GraphSpaceID spaceId,
                PartitionID partId,
                const HostAddr& src,
                const HostAddr& dst,
                kvstore::KVStore* kv,
                AdminClient* client)
        : balanceId_(balanceId)
        , spaceId_(spaceId)
        , partId_(partId)
        , src_(src)
        , dst_(dst)
        , taskIdStr_(buildTaskId())
        , kv_(kv)
        , client_(client) {}

    const std::string& taskIdStr() const {
        return taskIdStr_;
    }

    void invoke();

    void rollback();

    BalanceTaskResult result() const {
        return ret_;
    }

private:
    std::string buildTaskId() {
        return folly::stringPrintf("[%ld, %d:%d, %s:%d->%s:%d]",
                                   balanceId_,
                                   spaceId_,
                                   partId_,
                                   src_.host.c_str(),
                                   src_.port,
                                   dst_.host.c_str(),
                                   dst_.port);
    }

    bool saveInStore();

private:
    BalanceID    balanceId_;
    GraphSpaceID spaceId_;
    PartitionID  partId_;
    HostAddr     src_;
    HostAddr     dst_;
    std::string  taskIdStr_;
    kvstore::KVStore* kv_ = nullptr;
    AdminClient* client_ = nullptr;
    BalanceTaskStatus status_ = BalanceTaskStatus::START;
    BalanceTaskResult ret_ = BalanceTaskResult::IN_PROGRESS;
    int64_t startTimeMs_ = 0;
    int64_t endTimeMs_ = 0;
    std::function<void()> onFinished_;
    std::function<void()> onError_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_BALANCETASK_H_
