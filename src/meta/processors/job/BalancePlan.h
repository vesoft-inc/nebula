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

/**
 * @brief A balance plan contains some balance tasks, and could parallel run the tasks across parts
 */
class BalancePlan {
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

  /**
   * @brief Add a task
   *
   * @param task
   */
  void addTask(BalanceTask task) {
    tasks_.emplace_back(std::move(task));
  }

  void invoke();

  /**
   * @brief
   * TODO(heng): How to rollback if the some tasks failed.
   * For the tasks before UPDATE_META, they will go back to the original state
   * before balance. For the tasks after UPDATE_META, they will go on until
   * succeeded. NOTES: update_meta should be an atomic operation. There is no
   * middle state inside.
   */
  void rollback() {}

  meta::cpp2::JobStatus status() {
    return jobDescription_.getStatus();
  }

  void setStatus(meta::cpp2::JobStatus status) {
    jobDescription_.setStatus(status);
  }

  /**
   * @brief Save balance tasks into kvStore
   *
   * @return
   */
  nebula::cpp2::ErrorCode saveInStore();

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

  /**
   * @brief Load balance tasks from kvStore for rerun, rerun stopped or failed tasks if resume
   *
   * @param resume
   * @return
   */
  nebula::cpp2::ErrorCode recovery(bool resume = true);

  /**
   * @brief Dispatch tasks to buckets for parallel execution
   */
  void dispatchTasks();

  /**
   * @brief Load balance tasks from kvStore
   *
   * @param jobId The job that balance tasks belong to
   * @param kv The kvStore
   * @param client Client to make raft operation
   * @param resume If rerun the failed or stopped tasks
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<BalanceTask>> getBalanceTasks(
      JobID jobId, kvstore::KVStore* kv, AdminClient* client, bool resume = true);

  /**
   * @brief Load balance tasks and convert to cpp2::BalanceTask to make them serializable
   *
   * @param jobId
   * @param kv
   * @param client
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::BalanceTask>> show(JobID jobId,
                                                                               kvstore::KVStore* kv,
                                                                               AdminClient* client);

  /**
   * @brief Set a callback function, which would be called when job finished
   *
   * @param func
   */
  void setFinishCallBack(std::function<void(meta::cpp2::JobStatus)> func);

  template <typename InputIterator>
  void insertTask(InputIterator first, InputIterator last) {
    tasks_.insert(tasks_.end(), first, last);
  }

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
  std::atomic<int32_t> curIndex_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_BALANCEPLAN_H_
