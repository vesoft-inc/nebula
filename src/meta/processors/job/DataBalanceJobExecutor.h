/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DATABALANCEJOBEXECUTOR_H_
#define META_DATABALANCEJOBEXECUTOR_H_

#include "meta/processors/job/BalanceJobExecutor.h"

namespace nebula {
namespace meta {

class DataBalanceJobExecutor : public BalanceJobExecutor {
  FRIEND_TEST(BalanceTest, BalanceDataPlanTest);
  FRIEND_TEST(BalanceTest, NormalDataTest);
  FRIEND_TEST(BalanceTest, RecoveryTest);
  FRIEND_TEST(BalanceTest, StopPlanTest);

 public:
  DataBalanceJobExecutor(JobDescription jobDescription,
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

 private:
  std::vector<HostAddr> lostHosts_;
  JobDescription jobDescription_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DATABALANCEJOBEXECUTOR_H_
