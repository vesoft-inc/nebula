/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_BALANCEJOBEXECUTOR_H_
#define META_BALANCEJOBEXECUTOR_H_

#include "meta/processors/job/BalancePlan.h"
#include "meta/processors/job/BalanceTask.h"
#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

using ZoneParts = std::pair<std::string, std::vector<PartitionID>>;
using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;
using PartAllocation = std::unordered_map<PartitionID, std::vector<HostAddr>>;
using LeaderBalancePlan = std::vector<std::tuple<GraphSpaceID, PartitionID, HostAddr, HostAddr>>;
using ZoneNameAndParts = std::pair<std::string, std::vector<PartitionID>>;

class BalanceJobExecutor : public MetaJobExecutor {
  friend void testRestBlancer();

 public:
  BalanceJobExecutor(JobID jobId,
                     kvstore::KVStore* kvstore,
                     AdminClient* adminClient,
                     const std::vector<std::string>& params);

  bool check() override;

  nebula::cpp2::ErrorCode prepare() override;

  nebula::cpp2::ErrorCode stop() override;

  nebula::cpp2::ErrorCode finish(bool ret = true) override;

  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;

  bool runInMeta() override;

  nebula::cpp2::ErrorCode recovery() override;

 protected:
  nebula::cpp2::ErrorCode getAllSpaces(
      std::vector<std::tuple<GraphSpaceID, int32_t, bool>>& spaces);

  ErrorOr<nebula::cpp2::ErrorCode, bool> getHostParts(GraphSpaceID spaceId,
                                                      bool dependentOnGroup,
                                                      HostParts& hostParts,
                                                      int32_t& totalParts);

  void calDiff(const HostParts& hostParts,
               const std::vector<HostAddr>& activeHosts,
               std::vector<HostAddr>& expand,
               std::vector<HostAddr>& lost);

  nebula::cpp2::ErrorCode assembleZoneParts(const std::string& groupName, HostParts& hostParts);

  nebula::cpp2::ErrorCode collectZoneParts(const std::string& groupName, HostParts& hostParts);

  nebula::cpp2::ErrorCode save(const std::string& k, const std::string& v);

 protected:
  static std::atomic_bool running_;
  static std::mutex lock_;
  bool innerBalance_ = false;
  std::unique_ptr<folly::Executor> executor_;
  std::unordered_map<HostAddr, ZoneNameAndParts> zoneParts_;
  std::unordered_map<std::string, std::vector<HostAddr>> zoneHosts_;
  std::unordered_map<HostAddr, std::vector<PartitionID>> relatedParts_;
};

class DataBalanceJobExecutor : public BalanceJobExecutor {
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
  friend void testRestBlancer();

 public:
  DataBalanceJobExecutor(JobDescription jobDescription,
                         kvstore::KVStore* kvstore,
                         AdminClient* adminClient,
                         const std::vector<std::string>& params)
      : BalanceJobExecutor(jobDescription.getJobId(), kvstore, adminClient, params),
        jobDescription_(jobDescription) {}
  nebula::cpp2::ErrorCode recovery() override;
  nebula::cpp2::ErrorCode prepare() override;
  nebula::cpp2::ErrorCode finish(bool ret = true) override;
  nebula::cpp2::ErrorCode stop() override;

 protected:
  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;
  nebula::cpp2::ErrorCode buildBalancePlan();
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<BalanceTask>> genTasks(
      GraphSpaceID spaceId,
      int32_t spaceReplica,
      bool dependentOnGroup,
      std::vector<HostAddr>& lostHosts);
  ErrorOr<nebula::cpp2::ErrorCode, HostAddr> hostWithMinimalParts(const HostParts& hostParts,
                                                                  PartitionID partId);

  ErrorOr<nebula::cpp2::ErrorCode, HostAddr> hostWithMinimalPartsForZone(const HostAddr& source,
                                                                         const HostParts& hostParts,
                                                                         PartitionID partId);
  bool balanceParts(GraphSpaceID spaceId,
                    HostParts& confirmedHostParts,
                    int32_t totalParts,
                    std::vector<BalanceTask>& tasks,
                    bool dependentOnGroup);
  ErrorOr<nebula::cpp2::ErrorCode, std::pair<HostParts, std::vector<HostAddr>>> fetchHostParts(
      GraphSpaceID spaceId,
      bool dependentOnGroup,
      const HostParts& hostParts,
      std::vector<HostAddr>& lostHosts);
  nebula::cpp2::ErrorCode transferLostHost(std::vector<BalanceTask>& tasks,
                                           HostParts& confirmedHostParts,
                                           const HostAddr& source,
                                           GraphSpaceID spaceId,
                                           PartitionID partId,
                                           bool dependentOnGroup);
  Status checkReplica(const HostParts& hostParts,
                      const std::vector<HostAddr>& activeHosts,
                      int32_t replica,
                      PartitionID partId);
  std::vector<std::pair<HostAddr, int32_t>> sortedHostsByParts(const HostParts& hostParts);
  bool checkZoneLegal(const HostAddr& source, const HostAddr& target);
  nebula::cpp2::ErrorCode finishInternal(bool ret = true);

 private:
  static std::unique_ptr<BalancePlan> plan_;
  std::vector<HostAddr> lostHosts_;
  JobDescription jobDescription_;
};

class LeaderBalanceJobExecutor : public BalanceJobExecutor {
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
  friend void testRestBlancer();

 public:
  LeaderBalanceJobExecutor(JobID jobId,
                           kvstore::KVStore* kvstore,
                           AdminClient* adminClient,
                           const std::vector<std::string>& params)
      : BalanceJobExecutor(jobId, kvstore, adminClient, params) {}

 protected:
  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;

  ErrorOr<nebula::cpp2::ErrorCode, bool> buildLeaderBalancePlan(HostLeaderMap* hostLeaderMap,
                                                                GraphSpaceID spaceId,
                                                                int32_t replicaFactor,
                                                                bool dependentOnGroup,
                                                                LeaderBalancePlan& plan,
                                                                bool useDeviation = true);

  int32_t acquireLeaders(HostParts& allHostParts,
                         HostParts& leaderHostParts,
                         PartAllocation& peersMap,
                         std::unordered_set<HostAddr>& activeHosts,
                         const HostAddr& target,
                         LeaderBalancePlan& plan,
                         GraphSpaceID spaceId);

  int32_t giveupLeaders(HostParts& leaderParts,
                        PartAllocation& peersMap,
                        std::unordered_set<HostAddr>& activeHosts,
                        const HostAddr& source,
                        LeaderBalancePlan& plan,
                        GraphSpaceID spaceId);

  void simplifyLeaderBalnacePlan(GraphSpaceID spaceId, LeaderBalancePlan& plan);

 private:
  static std::atomic_bool inLeaderBalance_;
  std::unique_ptr<HostLeaderMap> hostLeaderMap_;
  std::unordered_map<HostAddr, std::pair<int32_t, int32_t>> hostBounds_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BALANCEJOBEXECUTOR_H_
