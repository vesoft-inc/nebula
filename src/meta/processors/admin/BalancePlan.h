/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADMIN_BALANCEPLAN_H_
#define META_ADMIN_BALANCEPLAN_H_

#include <gtest/gtest_prod.h>
#include "kvstore/KVStore.h"
#include "meta/processors/admin/BalanceTask.h"

namespace nebula {
namespace meta {

class BalancePlan {
    friend class Balancer;
    FRIEND_TEST(BalanceTest, BalancePlanTest);
    FRIEND_TEST(BalanceTest, NormalTest);
    FRIEND_TEST(BalanceTest, SpecifyHostTest);
    FRIEND_TEST(BalanceTest, SpecifyMultiHostTest);
    FRIEND_TEST(BalanceTest, MockReplaceMachineTest);
    FRIEND_TEST(BalanceTest, SingleReplicaTest);
    FRIEND_TEST(BalanceTest, RecoveryTest);
    FRIEND_TEST(BalanceTest, DispatchTasksTest);
    FRIEND_TEST(BalanceTest, StopBalanceDataTest);

public:
    enum class Status : uint8_t {
        NOT_START          = 0x01,
        IN_PROGRESS        = 0x02,
        SUCCEEDED          = 0x03,
        /**
         * TODO(heng): Currently, after the plan failed, we will try to resume it
         * when running "balance" again. But in many cases, the plan will be failed
         * forever, it this cases, we should rollback the plan.
         * */
        FAILED             = 0x04,
    };

    BalancePlan(BalanceID id, kvstore::KVStore* kv, AdminClient* client)
        : id_(id)
        , kv_(kv)
        , client_(client) {}

    BalancePlan(const BalancePlan& plan)
        : id_(plan.id_)
        , kv_(plan.kv_)
        , client_(plan.client_)
        , tasks_(plan.tasks_)
        , finishedTaskNum_(plan.finishedTaskNum_)
        , status_(plan.status_) {}

    void addTask(BalanceTask task) {
        tasks_.emplace_back(std::move(task));
    }

    void invoke();

    /**
     * TODO(heng): How to rollback if the some tasks failed.
     * For the tasks before UPDATE_META, they will go back to the original state before balance.
     * For the tasks after UPDATE_META, they will go on until succeeded.
     * NOTES: update_meta should be an atomic operation. There is no middle state inside.
     * */
    void rollback() {}

    Status status() {
        return status_;
    }

    cpp2::ErrorCode saveInStore(bool onlyPlan = false);

    BalanceID id() const {
        return id_;
    }

    const std::vector<BalanceTask>& tasks() const {
        return tasks_;
    }

    void stop() {
        std::lock_guard<std::mutex> lg(lock_);
        stopped_ = true;
    }

private:
    cpp2::ErrorCode recovery(bool resume = true);

    std::string planKey() const;

    std::string planVal() const;

    void dispatchTasks();

    static const std::string& prefix();

    static BalanceID id(const folly::StringPiece& rawKey);

    static BalancePlan::Status status(const folly::StringPiece& rawVal);

private:
    BalanceID id_ = 0;
    kvstore::KVStore* kv_ = nullptr;
    AdminClient* client_ = nullptr;
    std::vector<BalanceTask> tasks_;
    std::mutex lock_;
    size_t finishedTaskNum_ = 0;
    std::function<void()> onFinished_;
    Status status_ = Status::NOT_START;
    bool stopped_ = false;

    // List of task index in tasks_;
    using Bucket = std::vector<int32_t>;
    std::vector<Bucket> buckets_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_BALANCEPLAN_H_
