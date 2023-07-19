/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ADMIN_BALANCETASK_H_
#define META_ADMIN_BALANCETASK_H_

#include <gtest/gtest_prod.h>

#include "common/network/NetworkUtils.h"
#include "common/time/WallClock.h"
#include "kvstore/KVStore.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

/**
 * @brief A balance task mainly include jobID, spaceId, partId, srcHost, dstHost, status, result
 * The jobID, spaceId, partId, srcHost, dstHost make an unique id for the task
 */
class BalanceTask {
  friend class BalancePlan;
  FRIEND_TEST(BalanceTest, BalanceTaskTest);
  FRIEND_TEST(BalanceTest, BalancePlanTest);
  FRIEND_TEST(BalanceTest, SpecifyHostTest);
  FRIEND_TEST(BalanceTest, SpecifyMultiHostTest);
  FRIEND_TEST(BalanceTest, MockReplaceMachineTest);
  FRIEND_TEST(BalanceTest, SingleReplicaTest);
  FRIEND_TEST(BalanceTest, NormalTest);
  FRIEND_TEST(BalanceTest, TryToRecoveryTest);
  FRIEND_TEST(BalanceTest, RecoveryTest);
  FRIEND_TEST(BalanceTest, StopPlanTest);
  FRIEND_TEST(BalanceTest, BalanceZonePlanComplexTest);

 public:
  BalanceTask() = default;

  BalanceTask(JobID jobId,
              GraphSpaceID spaceId,
              PartitionID partId,
              const HostAddr& src,
              const HostAddr& dst,
              kvstore::KVStore* kv,
              AdminClient* client)
      : jobId_(jobId),
        spaceId_(spaceId),
        partId_(partId),
        src_(src),
        dst_(dst),
        kv_(kv),
        client_(client) {
    taskIdStr_ = buildTaskId();
    commandStr_ = buildCommand();
  }

  const std::string& taskIdStr() const {
    return taskIdStr_;
  }

  const std::string& taskCommandStr() const {
    return commandStr_;
  }

  /**
   * @brief Running this task
   */
  void invoke();

  void rollback();

  BalanceTaskResult result() const {
    return ret_;
  }

  const HostAddr& getSrcHost() const {
    return src_;
  }

  const HostAddr& getDstHost() const {
    return dst_;
  }

  void setSrcHost(const HostAddr& host) {
    src_ = host;
  }

  void setDstHost(const HostAddr& host) {
    dst_ = host;
  }

  PartitionID getPartId() const {
    return partId_;
  }

 private:
  std::string buildTaskId() {
    return folly::stringPrintf("%d, %d:%d", jobId_, spaceId_, partId_);
  }

  std::string buildCommand() {
    return folly::stringPrintf(
        "%s:%d->%s:%d", src_.host.c_str(), src_.port, dst_.host.c_str(), dst_.port);
  }

  /**
   * @brief Save this task into kvStore
   *
   * @return
   */
  bool saveInStore();

  int64_t startTime() const {
    return startTimeMs_;
  }

  int64_t endTime() const {
    return endTimeMs_;
  }

 private:
  JobID jobId_;
  GraphSpaceID spaceId_;
  PartitionID partId_;
  HostAddr src_;
  HostAddr dst_;
  std::string taskIdStr_;
  std::string commandStr_;
  kvstore::KVStore* kv_ = nullptr;
  AdminClient* client_ = nullptr;
  BalanceTaskStatus status_ = BalanceTaskStatus::START;
  BalanceTaskResult ret_ = BalanceTaskResult::IN_PROGRESS;
  int64_t startTimeMs_ = 0;
  int64_t endTimeMs_ = 0;
  std::function<void()> onFinished_;
  std::function<void()> onError_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_BALANCETASK_H_
