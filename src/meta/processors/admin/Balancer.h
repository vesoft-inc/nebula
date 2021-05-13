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
#include "common/network/NetworkUtils.h"
#include "common/time/WallClock.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/admin/BalanceTask.h"
#include "meta/processors/admin/BalancePlan.h"

namespace nebula {
namespace meta {

using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;
using PartAllocation = std::unordered_map<PartitionID, std::vector<HostAddr>>;
using LeaderBalancePlan = std::vector<std::tuple<GraphSpaceID, PartitionID, HostAddr, HostAddr>>;
using ZoneNameAndParts = std::pair<std::string, std::vector<PartitionID>>;

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
    FRIEND_TEST(BalanceTest, SimpleTestWithZone);
    FRIEND_TEST(BalanceTest, SpecifyHostTest);
    FRIEND_TEST(BalanceTest, SpecifyMultiHostTest);
    FRIEND_TEST(BalanceTest, MockReplaceMachineTest);
    FRIEND_TEST(BalanceTest, SingleReplicaTest);
    FRIEND_TEST(BalanceTest, TryToRecoveryTest);
    FRIEND_TEST(BalanceTest, RecoveryTest);
    FRIEND_TEST(BalanceTest, StopPlanTest);
    FRIEND_TEST(BalanceTest, CleanLastInvalidBalancePlanTest);
    FRIEND_TEST(BalanceTest, LeaderBalancePlanTest);
    FRIEND_TEST(BalanceTest, SimpleLeaderBalancePlanTest);
    FRIEND_TEST(BalanceTest, IntersectHostsLeaderBalancePlanTest);
    FRIEND_TEST(BalanceTest, LeaderBalanceTest);
    FRIEND_TEST(BalanceTest, ManyHostsLeaderBalancePlanTest);
    FRIEND_TEST(BalanceTest, LeaderBalanceWithZoneTest);
    FRIEND_TEST(BalanceTest, LeaderBalanceWithLargerZoneTest);
    FRIEND_TEST(BalanceTest, LeaderBalanceWithComplexZoneTest);
    FRIEND_TEST(BalanceTest, ExpansionZoneTest);
    FRIEND_TEST(BalanceTest, ExpansionHostIntoZoneTest);
    FRIEND_TEST(BalanceTest, ShrinkZoneTest);
    FRIEND_TEST(BalanceTest, ShrinkHostFromZoneTest);
    FRIEND_TEST(BalanceTest, BalanceWithComplexZoneTest);
    FRIEND_TEST(BalanceIntegrationTest, LeaderBalanceTest);
    FRIEND_TEST(BalanceIntegrationTest, BalanceTest);

public:
    static Balancer* instance(kvstore::KVStore* kv) {
        static std::unique_ptr<AdminClient> client(new AdminClient(kv));
        static std::unique_ptr<Balancer> balancer(new Balancer(kv, client.get()));
        return balancer.get();
    }

    ~Balancer() = default;

    /*
     * Return Error if reject the balance request, otherwise return balance id.
     * */
    ErrorOr<nebula::cpp2::ErrorCode, BalanceID>
    balance(std::vector<HostAddr>&& lostHosts = {});

    /**
     * Show balance plan id status.
     * */
    ErrorOr<nebula::cpp2::ErrorCode, BalancePlan> show(BalanceID id) const;

    /**
     * Stop balance plan by canceling all waiting balance task.
     * */
    ErrorOr<nebula::cpp2::ErrorCode, BalanceID> stop();

    /**
     * Clean invalid plan, return the invalid plan key if any
     * */
    ErrorOr<nebula::cpp2::ErrorCode, BalanceID> cleanLastInValidPlan();

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

    nebula::cpp2::ErrorCode leaderBalance();

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
    Balancer(kvstore::KVStore* kv, AdminClient* client)
        : kv_(kv)
        , client_(client) {
        executor_.reset(new folly::CPUThreadPoolExecutor(1));
    }
    /*
     * When the balancer failover, we should recovery the status.
     * */
    nebula::cpp2::ErrorCode recovery();

    /**
     * Build balance plan and save it in kvstore.
     * */
    nebula::cpp2::ErrorCode buildBalancePlan(std::vector<HostAddr>&& lostHosts);

    ErrorOr<nebula::cpp2::ErrorCode, std::vector<BalanceTask>>
    genTasks(GraphSpaceID spaceId,
             int32_t spaceReplica,
             bool dependentOnGroup,
             std::vector<HostAddr>&& lostHosts);

    ErrorOr<nebula::cpp2::ErrorCode, std::pair<HostParts, std::vector<HostAddr>>>
    fetchHostParts(GraphSpaceID spaceId,
                   bool dependentOnGroup,
                   const HostParts& hostParts,
                   std::vector<HostAddr>& lostHosts);

    ErrorOr<nebula::cpp2::ErrorCode, bool>
    getHostParts(GraphSpaceID spaceId,
                 bool dependentOnGroup,
                 HostParts& hostParts,
                 int32_t& totalParts);

    nebula::cpp2::ErrorCode
    assembleZoneParts(const std::string& groupName, HostParts& hostParts);

    void calDiff(const HostParts& hostParts,
                 const std::vector<HostAddr>& activeHosts,
                 std::vector<HostAddr>& newlyAdded,
                 std::vector<HostAddr>& lost);

    Status checkReplica(const HostParts& hostParts,
                        const std::vector<HostAddr>& activeHosts,
                        int32_t replica,
                        PartitionID partId);

    ErrorOr<nebula::cpp2::ErrorCode, HostAddr>
    hostWithMinimalParts(const HostParts& hostParts, PartitionID partId);

    ErrorOr<nebula::cpp2::ErrorCode, HostAddr>
    hostWithMinimalPartsForZone(const HostAddr& source,
                                const HostParts& hostParts,
                                PartitionID partId);
    bool balanceParts(BalanceID balanceId,
                      GraphSpaceID spaceId,
                      HostParts& newHostParts,
                      int32_t totalParts,
                      std::vector<BalanceTask>& tasks);

    nebula::cpp2::ErrorCode
    transferLostHost(std::vector<BalanceTask>& tasks,
                     HostParts& newHostParts,
                     const HostAddr& source,
                     GraphSpaceID spaceId,
                     PartitionID partId,
                     bool dependentOnGroup);

    std::vector<std::pair<HostAddr, int32_t>>
    sortedHostsByParts(const HostParts& hostParts);

    nebula::cpp2::ErrorCode
    getAllSpaces(std::vector<std::tuple<GraphSpaceID, int32_t, bool>>& spaces);

    ErrorOr<nebula::cpp2::ErrorCode, bool>
    buildLeaderBalancePlan(HostLeaderMap* hostLeaderMap,
                           GraphSpaceID spaceId,
                           int32_t replicaFactor,
                           bool dependentOnGroup,
                           LeaderBalancePlan& plan,
                           bool useDeviation = true);

    void simplifyLeaderBalnacePlan(GraphSpaceID spaceId, LeaderBalancePlan& plan);

    int32_t acquireLeaders(HostParts& allHostParts,
                           HostParts& leaderHostParts,
                           PartAllocation& peersMap,
                           std::unordered_set<HostAddr>& activeHosts,
                           const HostAddr& target,
                           LeaderBalancePlan& plan,
                           GraphSpaceID spaceId);

    int32_t giveupLeaders(HostParts& leaderHostParts,
                          PartAllocation& peersMap,
                          std::unordered_set<HostAddr>& activeHosts,
                          const HostAddr& source,
                          LeaderBalancePlan& plan,
                          GraphSpaceID spaceId);

    nebula::cpp2::ErrorCode
    collectZoneParts(const std::string& groupName, HostParts& hostParts);

    bool checkZoneLegal(const HostAddr& source, const HostAddr& target, PartitionID part);

private:
    std::atomic_bool running_{false};
    kvstore::KVStore* kv_{nullptr};
    AdminClient* client_{nullptr};
    // Current running plan.
    std::shared_ptr<BalancePlan> plan_{nullptr};
    std::unique_ptr<folly::Executor> executor_;
    std::atomic_bool inLeaderBalance_{false};

    // Host => Graph => Partitions
    std::unique_ptr<HostLeaderMap> hostLeaderMap_;
    mutable std::mutex lock_;

    std::unordered_map<HostAddr, std::pair<int32_t, int32_t>> hostBounds_;
    std::unordered_map<HostAddr, ZoneNameAndParts> zoneParts_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_BALANCERR_H_
