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
    FRIEND_TEST(BalanceTest, NormalTest);
    FRIEND_TEST(BalanceTest, RecoveryTest);

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
        , taskIdStr_(folly::stringPrintf(
                                      "[%ld, %d, %s:%d->%s:%d] ",
                                      balanceId,
                                      partId,
                                      network::NetworkUtils::intToIPv4(src.first).c_str(),
                                      src.second,
                                      network::NetworkUtils::intToIPv4(dst.first).c_str(),
                                      dst.second))
        , kv_(kv)
        , client_(client) {}

    const std::string& taskIdStr() const {
        return taskIdStr_;
    }

    void invoke();

    void rollback();

private:
    enum class Status : uint8_t {
        START               = 0x01,
        CHANGE_LEADER       = 0x02,
        ADD_PART_ON_DST     = 0x03,
        ADD_LEARNER         = 0x04,
        CATCH_UP_DATA       = 0x05,
        MEMBER_CHANGE       = 0x06,
        UPDATE_PART_META    = 0x07,  // After this state, we can't rollback anymore.
        REMOVE_PART_ON_SRC  = 0x08,
        END                 = 0xFF,
    };

    enum class Result : uint8_t {
        SUCCEEDED           = 0x01,
        FAILED              = 0x02,
        IN_PROGRESS         = 0x03,
    };

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
