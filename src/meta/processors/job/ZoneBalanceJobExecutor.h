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

 public:
  ZoneBalanceJobExecutor(JobDescription jobDescription,
                         kvstore::KVStore* kvstore,
                         AdminClient* adminClient,
                         const std::vector<std::string>& params)
      : BalanceJobExecutor(jobDescription.getJobId(), kvstore, adminClient, params),
        jobDescription_(jobDescription) {}
  nebula::cpp2::ErrorCode prepare() override;
  nebula::cpp2::ErrorCode stop() override;

 protected:
  folly::Future<Status> executeInternal() override;
  Status buildBalancePlan() override;
  nebula::cpp2::ErrorCode updateMeta();
  HostAddr insertPartIntoZone(std::map<std::string, std::vector<Host*>>* sortedZoneHosts,
                              Zone* zone,
                              PartitionID partId);
  nebula::cpp2::ErrorCode rebalanceActiveZones(
      std::vector<Zone*>* sortedActiveZones,
      std::map<std::string, std::vector<Host*>>* sortedZoneHosts,
      std::vector<BalanceTask>* tasks);

 private:
  std::vector<std::string> lostZones_;
  JobDescription jobDescription_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ZONEBALANCEJOBEXECUTOR_H_
