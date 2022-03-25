/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LEADERBALANCEJOBEXECUTOR_H_
#define META_LEADERBALANCEJOBEXECUTOR_H_

#include "meta/processors/job/BalancePlan.h"
#include "meta/processors/job/BalanceTask.h"
#include "meta/processors/job/MetaJobExecutor.h"
#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {
using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;
using PartAllocation = std::unordered_map<PartitionID, std::vector<HostAddr>>;
using LeaderBalancePlan = std::vector<std::tuple<GraphSpaceID, PartitionID, HostAddr, HostAddr>>;
using ZoneNameAndParts = std::pair<std::string, std::vector<PartitionID>>;

class LeaderBalanceJobExecutor : public MetaJobExecutor {
  FRIEND_TEST(BalanceTest, SimpleLeaderBalancePlanTest);
  FRIEND_TEST(BalanceTest, IntersectHostsLeaderBalancePlanTest);
  FRIEND_TEST(BalanceTest, ManyHostsLeaderBalancePlanTest);
  FRIEND_TEST(BalanceTest, LeaderBalanceTest);
  FRIEND_TEST(BalanceTest, LeaderBalanceWithZoneTest);
  FRIEND_TEST(BalanceTest, LeaderBalanceWithLargerZoneTest);
  FRIEND_TEST(BalanceTest, LeaderBalanceWithComplexZoneTest);

 public:
  LeaderBalanceJobExecutor(GraphSpaceID space,
                           JobID jobId,
                           kvstore::KVStore* kvstore,
                           AdminClient* adminClient,
                           const std::vector<std::string>& params);

  nebula::cpp2::ErrorCode finish(bool ret = true) override;

 protected:
  /**
   * @brief Build balance plan and run
   *
   * @return
   */
  folly::Future<Status> executeInternal() override;

  /**
   * @brief Build a plan to balance leader
   *
   * @param hostLeaderMap
   * @param spaceId
   * @param replicaFactor
   * @param dependentOnZone
   * @param plan
   * @param useDeviation
   * @return
   */
  ErrorOr<nebula::cpp2::ErrorCode, bool> buildLeaderBalancePlan(HostLeaderMap* hostLeaderMap,
                                                                GraphSpaceID spaceId,
                                                                int32_t replicaFactor,
                                                                bool dependentOnZone,
                                                                LeaderBalancePlan& plan,
                                                                bool useDeviation = true);

  /**
   * @brief Host will loop for the partition which is not leader, and try to acuire the leader
   *
   * @param allHostParts
   * @param leaderHostParts
   * @param peersMap
   * @param activeHosts
   * @param target
   * @param plan
   * @param spaceId
   * @return
   */
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

  void simplifyLeaderBalancePlan(GraphSpaceID spaceId, LeaderBalancePlan& plan);

  nebula::cpp2::ErrorCode getAllSpaces(
      std::vector<std::tuple<GraphSpaceID, int32_t, bool>>& spaces);

  ErrorOr<nebula::cpp2::ErrorCode, bool> getHostParts(GraphSpaceID spaceId,
                                                      bool dependentOnGroup,
                                                      HostParts& hostParts,
                                                      int32_t& totalParts);

  /**
   * @brief Compare and get the lost hosts and expand hosts
   *
   * @param hostParts
   * @param activeHosts
   * @param expand
   * @param lost
   */
  void calDiff(const HostParts& hostParts,
               const std::vector<HostAddr>& activeHosts,
               std::vector<HostAddr>& expand,
               std::vector<HostAddr>& lost);

  nebula::cpp2::ErrorCode assembleZoneParts(const std::vector<std::string>& zoneNames,
                                            HostParts& hostParts);

 private:
  std::atomic_bool inLeaderBalance_;
  std::unique_ptr<HostLeaderMap> hostLeaderMap_;
  std::unordered_map<HostAddr, std::pair<int32_t, int32_t>> hostBounds_;
  std::unordered_map<HostAddr, ZoneNameAndParts> zoneParts_;
  std::unordered_map<std::string, std::vector<HostAddr>> zoneHosts_;
  std::unordered_map<HostAddr, std::vector<PartitionID>> relatedParts_;
  std::unique_ptr<folly::Executor> executor_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LEADERBALANCEJOBEXECUTOR_H_
