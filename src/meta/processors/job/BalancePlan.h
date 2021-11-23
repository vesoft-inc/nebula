/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ADMIN_BALANCEPLAN_H_
#define META_ADMIN_BALANCEPLAN_H_

#include <gtest/gtest_prod.h>

#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"
#include "meta/processors/job/BalanceTask.h"
#include "meta/processors/job/JobDescription.h"

namespace nebula {
namespace meta {

class BalancePlan {
  friend class Balancer;
  friend class DataBalanceJobExecutor;
  FRIEND_TEST(BalanceTest, BalancePlanTest);
  FRIEND_TEST(BalanceTest, NormalTest);
  FRIEND_TEST(BalanceTest, SpecifyHostTest);
  FRIEND_TEST(BalanceTest, SpecifyMultiHostTest);
  FRIEND_TEST(BalanceTest, MockReplaceMachineTest);
  FRIEND_TEST(BalanceTest, SingleReplicaTest);
  FRIEND_TEST(BalanceTest, TryToRecoveryTest);
  FRIEND_TEST(BalanceTest, RecoveryTest);
  FRIEND_TEST(BalanceTest, DispatchTasksTest);
  FRIEND_TEST(BalanceTest, StopPlanTest);

 public:
  BalancePlan(JobDescription jobDescription, kvstore::KVStore* kv, AdminClient* client)
      : jobDescription_(jobDescription), kv_(kv), client_(client) {}

  BalancePlan(const BalancePlan& plan)
      : jobDescription_(plan.jobDescription_),
        kv_(plan.kv_),
        client_(plan.client_),
        tasks_(plan.tasks_),
        finishedTaskNum_(plan.finishedTaskNum_) {}

  void addTask(BalanceTask task) {
    tasks_.emplace_back(std::move(task));
  }

  void invoke();

  /**
   * TODO(heng): How to rollback if the some tasks failed.
   * For the tasks before UPDATE_META, they will go back to the original state
   * before balance. For the tasks after UPDATE_META, they will go on until
   * succeeded. NOTES: update_meta should be an atomic operation. There is no
   * middle state inside.
   * */
  void rollback() {}

  meta::cpp2::JobStatus status() {
    return jobDescription_.getStatus();
  }

  void setStatus(meta::cpp2::JobStatus status) {
    jobDescription_.setStatus(status);
  }

  nebula::cpp2::ErrorCode saveInStore(bool onlyPlan = false);

  JobID id() const {
    return jobDescription_.getJobId();
  }

  const std::vector<BalanceTask>& tasks() const {
    return tasks_;
  }

  int32_t taskSize() const {
    return tasks_.size();
  }

  void stop() {
    std::lock_guard<std::mutex> lg(lock_);
    stopped_ = true;
  }

  nebula::cpp2::ErrorCode recovery(bool resume = true);

  void dispatchTasks();

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<BalanceTask>> getBalanceTasks(
      JobID jobId, kvstore::KVStore* kv, AdminClient* client, bool resume = true);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::BalanceTask>> show(JobID jobId,
                                                                               kvstore::KVStore* kv,
                                                                               AdminClient* client);

  void setFinishCallBack(std::function<void(meta::cpp2::JobStatus)> func);

 private:
  JobDescription jobDescription_;
  kvstore::KVStore* kv_ = nullptr;
  AdminClient* client_ = nullptr;
  std::vector<BalanceTask> tasks_;
  std::mutex lock_;
  size_t finishedTaskNum_ = 0;
  std::function<void(meta::cpp2::JobStatus)> onFinished_;
  bool stopped_ = false;
  bool failed_ = false;

  // List of task index in tasks_;
  using Bucket = std::vector<int32_t>;
  std::vector<Bucket> buckets_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_BALANCEPLAN_H_
