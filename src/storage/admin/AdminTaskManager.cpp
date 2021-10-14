/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/AdminTaskManager.h"

#include <thrift/lib/cpp/protocol/TBinaryProtocol.h>
#include <thrift/lib/cpp/transport/TBufferTransports.h>

#include <tuple>

#include "storage/admin/AdminTask.h"
#include "storage/admin/AdminTaskProcessor.h"

DEFINE_uint32(max_concurrent_subtasks, 10, "The sub tasks could be invoked simultaneously");

namespace nebula {
namespace storage {

bool AdminTaskManager::init() {
  LOG(INFO) << "max concurrenct subtasks: " << FLAGS_max_concurrent_subtasks;
  auto threadFactory = std::make_shared<folly::NamedThreadFactory>("TaskManager");
  pool_ = std::make_unique<ThreadPool>(FLAGS_max_concurrent_subtasks, threadFactory);
  bgThread_ = std::make_unique<thread::GenericWorker>();
  if (!bgThread_->start()) {
    LOG(ERROR) << "background thread start failed";
    return false;
  }

  shutdown_.store(false, std::memory_order_release);
  bgThread_->addTask(&AdminTaskManager::schedule, this);
  ifAnyUnreported_ = true;
  handleUnreportedTasks();
  LOG(INFO) << "exit AdminTaskManager::init()";
  return true;
}

void AdminTaskManager::handleUnreportedTasks() {
  using futTuple =
      std::tuple<JobID, TaskID, std::string, folly::Future<StatusOr<nebula::cpp2::ErrorCode>>>;
  if (env_ == nullptr) return;
  unreportedAdminThread_.reset(new std::thread([this] {
    while (true) {
      std::unique_lock<std::mutex> lk(unreportedMutex_);
      if (!ifAnyUnreported_) {
        unreportedCV_.wait(lk);
      }
      if (shutdown_.load(std::memory_order_acquire)) {
        break;
      }
      ifAnyUnreported_ = false;
      std::unique_ptr<kvstore::KVIterator> iter;
      auto kvRet = env_->adminStore_->scan(&iter);
      lk.unlock();
      if (kvRet != nebula::cpp2::ErrorCode::SUCCEEDED || iter == nullptr) {
        continue;
      }
      std::vector<std::string> keys;
      std::vector<futTuple> futVec;
      for (; iter->valid(); iter->next()) {
        folly::StringPiece key = iter->key();
        int32_t seqId = *reinterpret_cast<const int32_t*>(key.data());
        if (seqId < 0) {
          continue;
        }
        JobID jobId = *reinterpret_cast<const JobID*>(key.data() + sizeof(int32_t));
        TaskID taskId =
            *reinterpret_cast<const TaskID*>(key.data() + sizeof(int32_t) + sizeof(JobID));
        folly::StringPiece val = iter->val();
        folly::StringPiece statsVal(val.data() + sizeof(nebula::cpp2::ErrorCode),
                                    val.size() - sizeof(nebula::cpp2::ErrorCode));
        nebula::meta::cpp2::StatsItem statsItem;
        apache::thrift::CompactSerializer::deserialize(statsVal, statsItem);
        nebula::meta::cpp2::JobStatus jobStatus = statsItem.get_status();
        nebula::cpp2::ErrorCode errCode =
            *reinterpret_cast<const nebula::cpp2::ErrorCode*>(val.data());
        meta::cpp2::StatsItem* pStats = nullptr;
        if (errCode == nebula::cpp2::ErrorCode::SUCCEEDED) {
          pStats = &statsItem;
        }
        LOG(INFO) << folly::sformat("reportTaskFinish(), job={}, task={}, rc={}",
                                    jobId,
                                    taskId,
                                    apache::thrift::util::enumNameSafe(errCode));
        if (seqId < env_->adminSeqId_) {
          if (jobStatus == nebula::meta::cpp2::JobStatus::RUNNING && pStats != nullptr) {
            pStats->set_status(nebula::meta::cpp2::JobStatus::FAILED);
          }
          auto fut = env_->metaClient_->reportTaskFinish(jobId, taskId, errCode, pStats);
          futVec.emplace_back(std::move(jobId), std::move(taskId), std::move(key), std::move(fut));
        } else if (jobStatus != nebula::meta::cpp2::JobStatus::RUNNING) {
          auto fut = env_->metaClient_->reportTaskFinish(jobId, taskId, errCode, pStats);
          futVec.emplace_back(std::move(jobId), std::move(taskId), std::move(key), std::move(fut));
        }
      }
      for (auto& p : futVec) {
        JobID jobId = std::get<0>(p);
        TaskID taskId = std::get<1>(p);
        std::string& key = std::get<2>(p);
        auto& fut = std::get<3>(p);
        auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
        fut.wait();
        if (!fut.hasValue()) {
          LOG(INFO) << folly::sformat(
              "reportTaskFinish() got rpc error:, job={}, task={}", jobId, taskId);
          ifAnyUnreported_ = true;
          continue;
        }
        if (!fut.value().ok()) {
          LOG(INFO) << folly::sformat("reportTaskFinish() has bad status:, job={}, task={}, rc={}",
                                      jobId,
                                      taskId,
                                      fut.value().status().toString());
          ifAnyUnreported_ = true;
          continue;
        }
        rc = fut.value().value();
        LOG(INFO) << folly::sformat("reportTaskFinish(), job={}, task={}, rc={}",
                                    jobId,
                                    taskId,
                                    apache::thrift::util::enumNameSafe(rc));
        if (rc == nebula::cpp2::ErrorCode::E_LEADER_CHANGED ||
            rc == nebula::cpp2::ErrorCode::E_STORE_FAILURE) {
          ifAnyUnreported_ = true;
          continue;
        } else {
          keys.emplace_back(key.data(), key.size());
          break;
        }
      }
      env_->adminStore_->multiRemove(keys);
    }
    LOG(INFO) << "Unreported-Admin-Thread stopped";
  }));
}

void AdminTaskManager::addAsyncTask(std::shared_ptr<AdminTask> task) {
  TaskHandle handle = std::make_pair(task->getJobId(), task->getTaskId());
  auto ret = tasks_.insert(handle, task).second;
  DCHECK(ret);
  taskQueue_.add(handle);
  LOG(INFO) << folly::stringPrintf("enqueue task(%d, %d), con req=%zu",
                                   task->getJobId(),
                                   task->getTaskId(),
                                   task->getConcurrentReq());
}

nebula::cpp2::ErrorCode AdminTaskManager::cancelJob(JobID jobId) {
  // When the job does not exist on the host,
  // it should return success instead of failure
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  auto it = tasks_.begin();
  while (it != tasks_.end()) {
    auto handle = it->first;
    if (handle.first == jobId) {
      it->second->cancel();
      removeTaskStatus(it->second->getJobId(), it->second->getTaskId());
      FLOG_INFO("task(%d, %d) cancelled", jobId, handle.second);
    }
    ++it;
  }
  return ret;
}

nebula::cpp2::ErrorCode AdminTaskManager::cancelTask(JobID jobId, TaskID taskId) {
  if (taskId < 0) {
    return cancelJob(jobId);
  }
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  TaskHandle handle = std::make_pair(jobId, taskId);
  auto it = tasks_.find(handle);
  if (it == tasks_.cend()) {
    ret = nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
  } else {
    it->second->cancel();
    removeTaskStatus(it->second->getJobId(), it->second->getTaskId());
  }
  return ret;
}

void AdminTaskManager::shutdown() {
  LOG(INFO) << "enter AdminTaskManager::shutdown()";
  shutdown_.store(true, std::memory_order_release);
  notifyReporting();
  bgThread_->stop();
  bgThread_->wait();

  for (auto it = tasks_.begin(); it != tasks_.end(); ++it) {
    it->second->cancel();  // cancelled_ = true;
  }

  pool_->join();
  if (unreportedAdminThread_ != nullptr) {
    unreportedAdminThread_->join();
  }
  if (env_) {
    env_->adminStore_.reset();
  }
  LOG(INFO) << "exit AdminTaskManager::shutdown()";
}

void AdminTaskManager::saveTaskStatus(JobID jobId,
                                      TaskID taskId,
                                      nebula::cpp2::ErrorCode rc,
                                      const nebula::meta::cpp2::StatsItem& result) {
  if (env_ == nullptr) return;
  std::string key = NebulaKeyUtils::adminTaskKey(env_->adminSeqId_, jobId, taskId);
  std::string val;
  val.append(reinterpret_cast<char*>(&rc), sizeof(nebula::cpp2::ErrorCode));
  std::string resVal;
  apache::thrift::CompactSerializer::serialize(result, &resVal);
  val.append(resVal);
  auto ret = env_->adminStore_->put(key, val);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(FATAL) << "Write put in admin-storage job id " << jobId << "task id " << taskId
               << " failed.";
  }
}

void AdminTaskManager::removeTaskStatus(JobID jobId, TaskID taskId) {
  if (env_ == nullptr) return;
  std::string key = NebulaKeyUtils::adminTaskKey(env_->adminSeqId_, jobId, taskId);
  env_->adminStore_->remove(key);
}

// schedule
void AdminTaskManager::schedule() {
  std::chrono::milliseconds interval{20};  // 20ms
  while (!shutdown_.load(std::memory_order_acquire)) {
    LOG(INFO) << "waiting for incoming task";
    folly::Optional<TaskHandle> optTaskHandle{folly::none};
    while (!optTaskHandle && !shutdown_.load(std::memory_order_acquire)) {
      optTaskHandle = taskQueue_.try_take_for(interval);
    }

    if (shutdown_.load(std::memory_order_acquire)) {
      LOG(INFO) << "detect AdminTaskManager::shutdown()";
      return;
    }

    auto handle = *optTaskHandle;
    LOG(INFO) << folly::stringPrintf("dequeue task(%d, %d)", handle.first, handle.second);
    auto it = tasks_.find(handle);
    if (it == tasks_.end()) {
      LOG(ERROR) << folly::stringPrintf(
          "trying to exec non-exist task(%d, %d)", handle.first, handle.second);
      continue;
    }

    auto task = it->second;
    auto errOrSubTasks = task->genSubTasks();
    if (!nebula::ok(errOrSubTasks)) {
      LOG(ERROR) << folly::sformat(
          "job {}, genSubTask failed, err={}",
          task->getJobId(),
          apache::thrift::util::enumNameSafe(nebula::error(errOrSubTasks)));
      task->finish(nebula::error(errOrSubTasks));
      tasks_.erase(handle);
      continue;
    }

    auto subTasks = nebula::value(errOrSubTasks);
    for (auto& subtask : subTasks) {
      task->subtasks_.add(subtask);
    }

    auto subTaskConcurrency =
        std::min(task->getConcurrentReq(), static_cast<size_t>(FLAGS_max_concurrent_subtasks));
    subTaskConcurrency = std::min(subTaskConcurrency, subTasks.size());
    task->unFinishedSubTask_ = subTasks.size();

    if (0 == subTasks.size()) {
      FLOG_INFO("task(%d, %d) finished, no subtask", task->getJobId(), task->getTaskId());
      task->finish();
      tasks_.erase(handle);
      continue;
    }

    FLOG_INFO("run task(%d, %d), %zu subtasks in %zu thread",
              handle.first,
              handle.second,
              task->unFinishedSubTask_.load(),
              subTaskConcurrency);
    for (size_t i = 0; i < subTaskConcurrency; ++i) {
      pool_->add(std::bind(&AdminTaskManager::runSubTask, this, handle));
    }
  }  // end while (!shutdown_)
  LOG(INFO) << "AdminTaskManager::pickTaskThread(~)";
}

void AdminTaskManager::runSubTask(TaskHandle handle) {
  auto it = tasks_.find(handle);
  if (it == tasks_.cend()) {
    FLOG_INFO("task(%d, %d) runSubTask() exit", handle.first, handle.second);
    return;
  }
  auto task = it->second;
  std::chrono::milliseconds take_dura{10};
  if (auto subTask = task->subtasks_.try_take_for(take_dura)) {
    if (task->status() == nebula::cpp2::ErrorCode::SUCCEEDED) {
      auto rc = nebula::cpp2::ErrorCode::E_UNKNOWN;
      try {
        rc = subTask->invoke();
      } catch (std::exception& ex) {
        LOG(ERROR) << folly::sformat(
            "task({}, {}) invoke() throw exception: {}", handle.first, handle.second, ex.what());
      } catch (...) {
        LOG(ERROR) << folly::sformat(
            "task({}, {}) invoke() throw unknown exception", handle.first, handle.second);
      }
      task->subTaskFinish(rc);
    }

    auto unFinishedSubTask = --task->unFinishedSubTask_;
    FLOG_INFO("subtask of task(%d, %d) finished, unfinished task %zu",
              task->getJobId(),
              task->getTaskId(),
              unFinishedSubTask);
    if (0 == unFinishedSubTask) {
      task->finish();
      tasks_.erase(handle);
    } else {
      pool_->add(std::bind(&AdminTaskManager::runSubTask, this, handle));
    }
  } else {
    FLOG_INFO("task(%d, %d) runSubTask() exit", handle.first, handle.second);
  }
}

void AdminTaskManager::notifyReporting() {
  std::unique_lock<std::mutex> lk(unreportedMutex_);
  ifAnyUnreported_ = true;
  unreportedCV_.notify_one();
}

void AdminTaskManager::saveAndNotify(JobID jobId,
                                     TaskID taskId,
                                     nebula::cpp2::ErrorCode rc,
                                     const nebula::meta::cpp2::StatsItem& result) {
  std::unique_lock<std::mutex> lk(unreportedMutex_);
  saveTaskStatus(jobId, taskId, rc, result);
  ifAnyUnreported_ = true;
  unreportedCV_.notify_one();
}

bool AdminTaskManager::isFinished(JobID jobID, TaskID taskID) {
  auto iter = tasks_.find(std::make_pair(jobID, taskID));
  // Task maybe erased when it's finished.
  if (iter == tasks_.cend()) {
    return true;
  }
  return iter->second->unFinishedSubTask_ == 0;
}

}  // namespace storage
}  // namespace nebula
