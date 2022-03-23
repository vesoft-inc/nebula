/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DATABALANCEJOBEXECUTOR_H_
#define META_DATABALANCEJOBEXECUTOR_H_

#include "meta/processors/job/BalanceJobExecutor.h"

namespace nebula {
namespace meta {

/**
 * @brief Executor for balance in zone, always called by job manager
 */
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
      : BalanceJobExecutor(
            jobDescription.getSpace(), jobDescription.getJobId(), kvstore, adminClient, params),
        jobDescription_(jobDescription) {}

  /**
   * @brief Parse paras to lost hosts
   *
   * @return
   */
  nebula::cpp2::ErrorCode prepare() override;

  /**
   * @brief Mark the job as stopped
   *
   * @return
   */
  nebula::cpp2::ErrorCode stop() override;

 protected:
  /**
   * @brief Build a balance plan and run
   *
   * @return
   */
  folly::Future<Status> executeInternal() override;

  /**
   * @brief Build a balance plan, which balance data in each zone
   * First, move parts from lost hosts to active hosts
   * Second, rebalance the active hosts in each zone
   *
   * @return
   */
  Status buildBalancePlan() override;

 private:
  std::vector<HostAddr> lostHosts_;
  JobDescription jobDescription_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DATABALANCEJOBEXECUTOR_H_
