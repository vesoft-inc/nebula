/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADMIN_BALANCERR_H_
#define META_ADMIN_BALANCERR_H_

#include <gtest/gtest_prod.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include "kvstore/KVStore.h"
#include "network/NetworkUtils.h"
#include "time/WallClock.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/admin/BalanceTask.h"
#include "meta/processors/admin/BalancePlan.h"

namespace nebula {
namespace meta {

using LeaderBalancePlan = std::vector<std::tuple<GraphSpaceID, PartitionID, HostAddr, HostAddr>>;

/**
There are two interfaces public:
 * Balance:  it will construct a balance plan and invoked it. If last balance plan is not succeeded, it will
 * try to resume it.
 *
 * Rollback: In many cases, if some plan failed forever, we call this interface to rollback.

Some notes:
1. Balance will generate balance plan according to current active hosts and parts allocation
2. For the plan, we hope after moving the least parts , it will reach a reasonable state.
3. Only one balance plan could be invoked at the same time.
4. Each balance plan has one id, and we could show the status by "balance id" command
   and after FO, we could resume the balance plan by type "balance" again.
5. Each balance plan contains many balance tasks, the task represents the minimum movement unit.
6. We save the whole balancePlan state in kvstore to do failover.
7. Each balance task contains serval steps. And it should be executed step by step.
8. One task failed will result in the whole balance plan failed.
9. Currently, we hope tasks for the same part could be invoked serially
 * */
class Balancer {
    FRIEND_TEST(BalanceTest, BalancePartsTest);
    FRIEND_TEST(BalanceTest, NormalTest);
    FRIEND_TEST(BalanceTest, SpecifyHostTest);
    FRIEND_TEST(BalanceTest, SpecifyMultiHostTest);
    FRIEND_TEST(BalanceTest, MockReplaceMachineTest);
    FRIEND_TEST(BalanceTest, SingleReplicaTest);
    FRIEND_TEST(BalanceTest, RecoveryTest);
    FRIEND_TEST(BalanceTest, StopBalanceDataTest);
    FRIEND_TEST(BalanceTest, LeaderBalancePlanTest);
    FRIEND_TEST(BalanceTest, SimpleLeaderBalancePlanTest);
    FRIEND_TEST(BalanceTest, IntersectHostsLeaderBalancePlanTest);
    FRIEND_TEST(BalanceTest, LeaderBalanceTest);
    FRIEND_TEST(BalanceTest, ManyHostsLeaderBalancePlanTest);
    FRIEND_TEST(BalanceIntegrationTest, LeaderBalanceTest);
    FRIEND_TEST(BalanceIntegrationTest, BalanceTest);

public:
    static Balancer* instance(kvstore::KVStore* kv) {
        static std::unique_ptr<AdminClient> client(new AdminClient(kv));
        static std::unique_ptr<Balancer> balancer(new Balancer(kv, std::move(client)));
        return balancer.get();
    }

    ~Balancer() = default;

    /*
     * Return Error if reject the balance request, otherwise return balance id.
     * */
    ErrorOr<cpp2::ErrorCode, BalanceID> balance(std::unordered_set<HostAddr> hostDel = {});

    /**
     * Show balance plan id status.
     * */
    StatusOr<BalancePlan> show(BalanceID id) const;

    /**
     * Stop balance plan by canceling all waiting balance task.
     * */
    StatusOr<BalanceID> stop();

    /**
     * TODO(heng): rollback some balance plan.
     */
    Status rollback(BalanceID id) {
        return Status::Error("unplemented, %ld", id);
    }

    /**
     * TODO(heng): Execute balance plan from outside.
     * */
    Status execute(BalancePlan plan) {
        UNUSED(plan);
        return Status::Error("Unsupport it yet!");
    }

    /**
     * TODO(heng): Execute specific balance plan by id.
     * */
    Status execute(BalanceID id) {
        UNUSED(id);
        return Status::Error("Unsupport it yet!");
    }

    cpp2::ErrorCode leaderBalance();

    void finish() {
        CHECK(!lock_.try_lock());
        plan_.reset();
        running_ = false;
    }

    bool isRunning() {
        std::lock_guard<std::mutex> lg(lock_);
        return running_;
    }

private:
    Balancer(kvstore::KVStore* kv, std::unique_ptr<AdminClient> client)
        : kv_(kv)
        , client_(std::move(client)) {
        executor_.reset(new folly::CPUThreadPoolExecutor(1));
    }
    /*
     * When the balancer failover, we should recovery the status.
     * */
    cpp2::ErrorCode recovery();

    /**
     * Build balance plan and save it in kvstore.
     * */
    cpp2::ErrorCode buildBalancePlan(std::unordered_set<HostAddr> hostDel);

    ErrorOr<cpp2::ErrorCode, std::vector<BalanceTask>>
    genTasks(GraphSpaceID spaceId, int32_t spaceReplica, std::unordered_set<HostAddr> hostDel);

    void getHostParts(GraphSpaceID spaceId,
                      std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
                      int32_t& totalParts);

    void calDiff(const std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
                 const std::vector<HostAddr>& activeHosts,
                 std::vector<HostAddr>& newlyAdded,
                 std::unordered_set<HostAddr>& lost);

    Status checkReplica(const std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
                        const std::vector<HostAddr>& activeHosts,
                        int32_t replica,
                        PartitionID partId);

    StatusOr<HostAddr> hostWithMinimalParts(
                        const std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
                        PartitionID partId);

    void balanceParts(BalanceID balanceId,
                      GraphSpaceID spaceId,
                      std::unordered_map<HostAddr, std::vector<PartitionID>>& newHostParts,
                      int32_t totalParts,
                      std::vector<BalanceTask>& tasks);


    std::vector<std::pair<HostAddr, int32_t>>
    sortedHostsByParts(const std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts);

    bool getAllSpaces(std::vector<std::pair<GraphSpaceID, int32_t>>& spaces,
                      kvstore::ResultCode& retCode);

    std::unordered_map<HostAddr, std::vector<PartitionID>>
    buildLeaderBalancePlan(HostLeaderMap* hostLeaderMap, GraphSpaceID spaceId,
                           LeaderBalancePlan& plan, bool useDeviation = true);

    void simplifyLeaderBalnacePlan(GraphSpaceID spaceId, LeaderBalancePlan& plan);

    int32_t acquireLeaders(std::unordered_map<HostAddr, std::vector<PartitionID>>& allHostParts,
                           std::unordered_map<HostAddr, std::vector<PartitionID>>& leaderHostParts,
                           std::unordered_map<PartitionID, std::vector<HostAddr>>& peersMap,
                           std::unordered_set<HostAddr>& activeHosts,
                           HostAddr to,
                           size_t maxLoad,
                           LeaderBalancePlan& plan,
                           GraphSpaceID spaceId);

    int32_t giveupLeaders(std::unordered_map<HostAddr, std::vector<PartitionID>>& leaderHostParts,
                          std::unordered_map<PartitionID, std::vector<HostAddr>>& peersMap,
                          std::unordered_set<HostAddr>& activeHosts,
                          HostAddr from,
                          size_t maxLoad,
                          LeaderBalancePlan& plan,
                          GraphSpaceID spaceId);

private:
    std::atomic_bool running_{false};
    kvstore::KVStore* kv_ = nullptr;
    std::unique_ptr<AdminClient> client_{nullptr};
    // Current running plan.
    std::shared_ptr<BalancePlan> plan_{nullptr};
    std::unique_ptr<folly::Executor> executor_;
    std::atomic_bool inLeaderBalance_{false};
    std::unique_ptr<HostLeaderMap> hostLeaderMap_;
    mutable std::mutex lock_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_BALANCERR_H_
