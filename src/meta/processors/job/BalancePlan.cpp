/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/BalancePlan.h"

#include <folly/synchronization/Baton.h>

#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"

DEFINE_uint32(task_concurrency, 10, "The tasks number could be invoked simultaneously");

namespace nebula {
namespace meta {

void BalancePlan::dispatchTasks() {
  // Key -> spaceID + partID,  Val -> List of task index in tasks_;
  std::unordered_map<std::pair<GraphSpaceID, PartitionID>, std::vector<int32_t>> partTasks;
  int32_t index = 0;
  for (auto& task : tasks_) {
    partTasks[std::make_pair(task.spaceId_, task.partId_)].emplace_back(index++);
  }
  buckets_.resize(partTasks.size());
  int32_t bucketIndex = 0;
  for (auto it = partTasks.begin(); it != partTasks.end(); it++) {
    for (auto taskIndex : it->second) {
      buckets_[bucketIndex].emplace_back(taskIndex);
    }
    bucketIndex++;
  }
}

folly::Future<meta::cpp2::JobStatus> BalancePlan::invoke() {
  auto retFuture = promise_.getFuture();

  // Sort the tasks by its id to ensure the order after recovery.
  setStatus(meta::cpp2::JobStatus::RUNNING);
  std::sort(
      tasks_.begin(), tasks_.end(), [](auto& l, auto& r) { return l.taskIdStr() < r.taskIdStr(); });
  dispatchTasks();
  for (size_t i = 0; i < buckets_.size(); i++) {
    for (size_t j = 0; j < buckets_[i].size(); j++) {
      auto taskIndex = buckets_[i][j];
      tasks_[taskIndex].onFinished_ = [this, i, j]() {
        bool finished = false;
        bool stopped = false;
        {
          std::lock_guard<std::mutex> lg(lock_);
          finishedTaskNum_++;
          LOG(INFO) << "Balance " << id() << " has completed " << finishedTaskNum_ << " task";
          if (finishedTaskNum_ == tasks_.size()) {
            finished = true;
            if (status() == meta::cpp2::JobStatus::RUNNING) {
              setStatus(meta::cpp2::JobStatus::FINISHED);
              LOG(INFO) << "Balance " << id() << " succeeded!";
            }
          }
          stopped = stopped_;
        }
        if (finished) {
          CHECK_EQ(j, buckets_[i].size() - 1);
          saveInStore();
          VLOG(4) << "BalancePlan::invoke finish";
          auto result =
              stopped ? meta::cpp2::JobStatus::STOPPED
                      : failed_ ? meta::cpp2::JobStatus::FAILED : meta::cpp2::JobStatus::FINISHED;
          promise_.setValue(result);
        } else if (j + 1 < buckets_[i].size()) {
          auto& task = tasks_[buckets_[i][j + 1]];
          if (stopped) {
            task.ret_ = BalanceTaskResult::INVALID;
          }
          task.invoke();
        } else {
          size_t index = curIndex_.fetch_add(1, std::memory_order_relaxed);
          if (index < buckets_.size()) {
            Bucket& bucket = buckets_[index];
            if (!bucket.empty()) {
              auto& task = tasks_[bucket[0]];
              if (stopped) {
                task.ret_ = BalanceTaskResult::INVALID;
              }
              task.invoke();
            }
          }
        }
      };  // onFinished

      tasks_[taskIndex].onError_ = [this, i, j]() {
        bool finished = false;
        bool stopped = false;
        {
          std::lock_guard<std::mutex> lg(lock_);
          finishedTaskNum_++;
          failed_ = true;
          LOG(INFO) << "Balance " << id() << " has completed " << finishedTaskNum_ << " task";
          setStatus(meta::cpp2::JobStatus::FAILED);
          if (finishedTaskNum_ == tasks_.size()) {
            finished = true;
            LOG(INFO) << "Balance " << id() << " failed!";
          }
          stopped = stopped_;
        }
        if (finished) {
          CHECK_EQ(j, buckets_[i].size() - 1);
          VLOG(4) << "BalancePlan::invoke finish";
          auto result = stopped ? meta::cpp2::JobStatus::STOPPED : meta::cpp2::JobStatus::FAILED;
          promise_.setValue(result);
        } else if (j + 1 < buckets_[i].size()) {
          auto& task = tasks_[buckets_[i][j + 1]];
          LOG(INFO) << "Skip the task for the same partId " << task.partId_;
          if (stopped) {
            task.ret_ = BalanceTaskResult::INVALID;
          } else {
            task.ret_ = BalanceTaskResult::FAILED;
          }
          task.invoke();
        } else {
          size_t index = curIndex_.fetch_add(1, std::memory_order_relaxed);
          if (index < buckets_.size()) {
            Bucket& bucket = buckets_[index];
            if (!bucket.empty()) {
              auto& task = tasks_[bucket[0]];
              if (stopped) {
                task.ret_ = BalanceTaskResult::INVALID;
              }
              task.invoke();
            }
          }
        }
      };  // onError
    }     // for (auto j = 0; j < buckets_[i].size(); j++)
  }       // for (auto i = 0; i < buckets_.size(); i++)

  saveInStore();
  uint32 bucketSize = buckets_.size();
  int32_t concurrency = std::min(FLAGS_task_concurrency, bucketSize);
  curIndex_.store(concurrency, std::memory_order_relaxed);
  for (int32_t i = 0; i < concurrency; i++) {
    if (!buckets_[i].empty()) {
      tasks_[buckets_[i][0]].invoke();
    }
  }
  return retFuture;
}

nebula::cpp2::ErrorCode BalancePlan::saveInStore() {
  CHECK_NOTNULL(kv_);
  std::vector<kvstore::KV> data;
  for (auto& task : tasks_) {
    data.emplace_back(
        MetaKeyUtils::balanceTaskKey(
            task.jobId_, task.spaceId_, task.partId_, task.src_, task.dst_),
        MetaKeyUtils::balanceTaskVal(task.status_, task.ret_, task.startTimeMs_, task.endTimeMs_));
  }
  folly::Baton<true, std::atomic> baton;
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  kv_->asyncMultiPut(kDefaultSpaceId,
                     kDefaultPartId,
                     std::move(data),
                     [&baton, &ret](nebula::cpp2::ErrorCode code) {
                       if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                         ret = code;
                         LOG(INFO)
                             << "Can't write the kvstore, ret = " << static_cast<int32_t>(code);
                       }
                       baton.post();
                     });
  baton.wait();
  return ret;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::BalanceTask>> BalancePlan::show(
    JobID jobId, kvstore::KVStore* kv, AdminClient* client) {
  auto ret = getBalanceTasks(jobId, kv, client, false);
  if (!ok(ret)) {
    return error(ret);
  }
  std::vector<BalanceTask> tasks = value(ret);
  std::vector<cpp2::BalanceTask> thriftTasks;
  for (auto& task : tasks) {
    cpp2::BalanceTask t;
    t.id_ref() = task.taskIdStr();
    t.command_ref() = task.taskCommandStr();
    switch (task.result()) {
      case BalanceTaskResult::SUCCEEDED:
        t.result_ref() = cpp2::TaskResult::SUCCEEDED;
        break;
      case BalanceTaskResult::FAILED:
        t.result_ref() = cpp2::TaskResult::FAILED;
        break;
      case BalanceTaskResult::IN_PROGRESS:
        t.result_ref() = cpp2::TaskResult::IN_PROGRESS;
        break;
      case BalanceTaskResult::INVALID:
        t.result_ref() = cpp2::TaskResult::INVALID;
        break;
    }
    t.start_time_ref() = task.startTime();
    t.stop_time_ref() = task.endTime();
    thriftTasks.emplace_back(std::move(t));
  }
  return thriftTasks;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<BalanceTask>> BalancePlan::getBalanceTasks(
    JobID jobId, kvstore::KVStore* kv, AdminClient* client, bool resume) {
  CHECK_NOTNULL(kv);
  const auto& prefix = MetaKeyUtils::balanceTaskPrefix(jobId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Can't access kvstore, ret = " << static_cast<int32_t>(ret);
    return ret;
  }
  std::vector<BalanceTask> tasks;
  while (iter->valid()) {
    BalanceTask task;
    task.kv_ = kv;
    task.client_ = client;
    {
      auto tup = MetaKeyUtils::parseBalanceTaskKey(iter->key());
      task.jobId_ = std::get<0>(tup);
      task.spaceId_ = std::get<1>(tup);
      task.partId_ = std::get<2>(tup);
      task.src_ = std::get<3>(tup);
      task.dst_ = std::get<4>(tup);
      task.taskIdStr_ = task.buildTaskId();
      task.commandStr_ = task.buildCommand();
    }
    {
      auto tup = MetaKeyUtils::parseBalanceTaskVal(iter->val());
      task.status_ = std::get<0>(tup);
      task.ret_ = std::get<1>(tup);
      task.startTimeMs_ = std::get<2>(tup);
      task.endTimeMs_ = std::get<3>(tup);
      if (resume && task.ret_ != BalanceTaskResult::SUCCEEDED) {
        // Resume the failed or invalid task, skip the in-progress tasks
        if (task.ret_ == BalanceTaskResult::FAILED || task.ret_ == BalanceTaskResult::INVALID) {
          task.ret_ = BalanceTaskResult::IN_PROGRESS;
          task.status_ = BalanceTaskStatus::START;
        }
        auto activeHostRet = ActiveHostsMan::isLived(kv, task.dst_);
        if (!nebula::ok(activeHostRet)) {
          auto retCode = nebula::error(activeHostRet);
          LOG(INFO) << "Get active hosts failed, error: " << static_cast<int32_t>(retCode);
          return retCode;
        } else {
          auto isLive = nebula::value(activeHostRet);
          if (!isLive) {
            LOG(INFO) << "The destination is not lived";
            task.ret_ = BalanceTaskResult::INVALID;
          }
        }
        task.endTimeMs_ = 0;
      }
    }
    tasks.emplace_back(std::move(task));
    iter->next();
  }
  return tasks;
}

nebula::cpp2::ErrorCode BalancePlan::recovery(bool resume) {
  auto ret = getBalanceTasks(id(), kv_, client_, resume);
  if (!ok(ret)) {
    return error(ret);
  }
  tasks_ = value(ret);
  return tasks_.empty() ? nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND
                        : nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
