/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADMIN_BALANCETASK_H_
#define META_ADMIN_BALANCETASK_H_

#include <gtest/gtest_prod.h>
#include "meta/ActiveHostsMan.h"
#include "time/WallClock.h"
#include "kvstore/KVStore.h"
#include "network/NetworkUtils.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

class BalanceTask {
    friend class BalancePlan;
    FRIEND_TEST(BalanceTaskTest, SimpleTest);
    FRIEND_TEST(BalanceTest, BalancePlanTest);
    FRIEND_TEST(BalanceTest, SpecifyHostTest);
    FRIEND_TEST(BalanceTest, SpecifyMultiHostTest);
    FRIEND_TEST(BalanceTest, MockReplaceMachineTest);
    FRIEND_TEST(BalanceTest, SingleReplicaTest);
    FRIEND_TEST(BalanceTest, NormalTest);
    FRIEND_TEST(BalanceTest, RecoveryTest);
    FRIEND_TEST(BalanceTest, StopBalanceDataTest);

public:
    enum class Status : uint8_t {
        START                   = 0x01,
        CHANGE_LEADER           = 0x02,
        ADD_PART_ON_DST         = 0x03,
        ADD_LEARNER             = 0x04,
        CATCH_UP_DATA           = 0x05,
        MEMBER_CHANGE_ADD       = 0x06,
        MEMBER_CHANGE_REMOVE    = 0x07,
        UPDATE_PART_META        = 0x08,  // After this state, we can't rollback anymore.
        REMOVE_PART_ON_SRC      = 0x09,
        CHECK                   = 0x0A,
        END                     = 0xFF,
    };

    enum class Result : uint8_t {
        SUCCEEDED           = 0x01,
        FAILED              = 0x02,
        IN_PROGRESS         = 0x03,
        INVALID             = 0x04,
    };

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

    Result result() const {
        return ret_;
    }

private:
    std::string buildTaskId() {
        return folly::stringPrintf("[%ld, %d:%d, %s:%d->%s:%d] ",
                                   balanceId_,
                                   spaceId_,
                                   partId_,
                                   network::NetworkUtils::intToIPv4(src_.first).c_str(),
                                   src_.second,
                                   network::NetworkUtils::intToIPv4(dst_.first).c_str(),
                                   dst_.second);
    }

    bool saveInStore();

    std::string taskKey();

    std::string taskVal();

    static std::string prefix(BalanceID balanceId);

    static std::tuple<BalanceID, GraphSpaceID, PartitionID, HostAddr, HostAddr>
    parseKey(const folly::StringPiece& rawKey);

    static std::tuple<BalanceTask::Status, BalanceTask::Result, int64_t, int64_t>
    parseVal(const folly::StringPiece& rawVal);

private:
    BalanceID    balanceId_;
    GraphSpaceID spaceId_;
    PartitionID  partId_;
    HostAddr     src_;
    HostAddr     dst_;
    std::string  taskIdStr_;
    kvstore::KVStore* kv_ = nullptr;
    AdminClient* client_ = nullptr;
    Status       status_ = Status::START;
    Result       ret_ = Result::IN_PROGRESS;
    int64_t      startTimeMs_ = 0;
    int64_t      endTimeMs_ = 0;
    std::function<void()> onFinished_;
    std::function<void()> onError_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_BALANCETASK_H_
