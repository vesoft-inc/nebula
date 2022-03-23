/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ZONEBALANCEJOBEXECUTOR_H_
#define META_ZONEBALANCEJOBEXECUTOR_H_

#include "meta/processors/job/BalanceJobExecutor.h"

namespace nebula {
namespace meta {

class ZoneBalanceJobExecutor : public BalanceJobExecutor {
  FRIEND_TEST(BalanceTest, RemoveZonePlanTest);
  FRIEND_TEST(BalanceTest, BalanceZonePlanTest);
  FRIEND_TEST(BalanceTest, BalanceZoneRemainderPlanTest);
  FRIEND_TEST(BalanceTest, NormalZoneTest);
  FRIEND_TEST(BalanceTest, StopPlanTest);
  FRIEND_TEST(BalanceTest, BalanceZonePlanComplexTest);
  FRIEND_TEST(BalanceTest, NormalZoneComplexTest);

 public:
  ZoneBalanceJobExecutor(JobDescription jobDescription,
                         kvstore::KVStore* kvstore,
                         AdminClient* adminClient,
                         const std::vector<std::string>& params)
      : BalanceJobExecutor(
            jobDescription.getSpace(), jobDescription.getJobId(), kvstore, adminClient, params),
        jobDescription_(jobDescription) {}

  nebula::cpp2::ErrorCode prepare() override;
  nebula::cpp2::ErrorCode stop() override;

 protected:
  folly::Future<Status> executeInternal() override;

  /**
   * @brief
   * First, move the lostZones' parts to the active zones
   * Second, make balance for the active zones
   *
   * @return
   */
  Status buildBalancePlan() override;

  /**
   * @brief If removed zones, update zone-space info when job finished
   *
   * @return
   */
  nebula::cpp2::ErrorCode updateMeta();

  /**
   * @brief For a given zone, choose a host of the zone to insert the given part
   *
   * @param sortedZoneHosts
   * @param zone
   * @param partId
   * @return
   */
  HostAddr insertPartIntoZone(std::map<std::string, std::vector<Host*>>* sortedZoneHosts,
                              Zone* zone,
                              PartitionID partId);

  /**
   * @brief Give a zone vector sorted by part number, make balance for each zone
   *
   * @param sortedActiveZones
   * @param sortedZoneHosts
   * @param tasks
   * @return
   */
  nebula::cpp2::ErrorCode rebalanceActiveZones(
      std::vector<Zone*>* sortedActiveZones,
      std::map<std::string, std::vector<Host*>>* sortedZoneHosts,
      std::map<PartitionID, std::vector<BalanceTask>>* existTasks);

 private:
  std::vector<std::string> lostZones_;
  JobDescription jobDescription_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ZONEBALANCEJOBEXECUTOR_H_
