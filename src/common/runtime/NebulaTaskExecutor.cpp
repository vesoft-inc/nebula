/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/runtime/NebulaTaskExecutor.h"

#include <folly/ScopeGuard.h>

#include <atomic>
#include <list>
#include <shared_mutex>
#include <thread>

#include "common/base/Logging.h"

DEFINE_int32(max_threads_per_query,
             std::thread::hardware_concurrency(),
             "Max allowed used threads");

namespace nebula {

// This TaskManager will store all jobs submitted by a special query and manage the max parallelism
// when running these tasks. It limits the used threads of a query by controlling the number of
// submitted jobs.
class NebulaTaskExecutor::TaskManager {
 public:
  explicit TaskManager(folly::Executor* executor)
      : maxThreads_(FLAGS_max_threads_per_query), executor_(DCHECK_NOTNULL(executor)) {
    if (maxThreads_ <= 0) {
      maxThreads_ = std::numeric_limits<int64_t>::max();
    }
  }

  ~TaskManager() = default;

  void push(folly::Func f) {
    std::unique_lock<std::shared_mutex> l(lock_);
    tasks_.emplace_back(std::move(f));
  }

  void notify() {
    if (numUsedThreads_.load() < maxThreads_) {
      std::unique_lock<std::shared_mutex> l(lock_);
      if (numUsedThreads_.load() < maxThreads_) {
        addTask();
      }
    }
  }

 private:
  void addTask() {
    if (!tasks_.empty()) {
      folly::Func task = std::move(tasks_.front());
      numUsedThreads_.fetch_add(1);

      // The task is the callback wrapper of folly::Future for the following purposes:
      //  1. notify TaskManager to add a pending job to the real folly::Executor.
      executor_->add([task = std::move(task), this]() mutable {
        DCHECK_LE(numUsedThreads_, maxThreads_);

        SCOPE_EXIT {
          numUsedThreads_.fetch_sub(1);

          notify();
        };

        task();
      });
      tasks_.pop_front();
    }
  }

  int64_t maxThreads_;
  std::atomic<int> numUsedThreads_{0};
  folly::Executor* executor_{nullptr};

  std::shared_mutex lock_;
  std::list<folly::Func> tasks_;
};

NebulaTaskExecutor::NebulaTaskExecutor(folly::Executor* proxy) {
  taskMgr_ = std::make_unique<TaskManager>(proxy);
}

NebulaTaskExecutor::~NebulaTaskExecutor() {}

// Tasks submitted by Future only use this add function
void NebulaTaskExecutor::add(folly::Func f) {
  taskMgr_->push(std::move(f));
  taskMgr_->notify();
}

}  // namespace nebula
