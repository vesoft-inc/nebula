/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest_prod.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

#include "common/base/Base.h"

namespace nebula {
namespace cpu {

class CpuBinder {
  FRIEND_TEST(CpuBinderTest, Spawn);
  FRIEND_TEST(CpuBinderTest, StdThreadExclusiveBindBySelf);
  FRIEND_TEST(CpuBinderTest, StdThreadExclusiveBindByOthers);
  FRIEND_TEST(CpuBinderTest, StdThreadShareBindBySelf);
  FRIEND_TEST(CpuBinderTest, StdThreadShareBindByOthers);
  FRIEND_TEST(CpuBinderTest, GenericPoolExclusiveBindByOthers);
  FRIEND_TEST(CpuBinderTest, GenericPoolShareBindByOthers);
  FRIEND_TEST(CpuBinderTest, IOThreadPoolExclusiveBindByOthers);
  FRIEND_TEST(CpuBinderTest, IOThreadPoolShareBindByOthers);
  FRIEND_TEST(CpuBinderTest, ThreadManagerExclusiveBindByOthers);
  FRIEND_TEST(CpuBinderTest, ThreadManagerShareBindByOthers);

  using PriorityThreadManager = apache::thrift::concurrency::PriorityThreadManager;
  using PRIORITY = apache::thrift::concurrency::PRIORITY;

 public:
  enum Policy { kShared, kExclusive };

  // whether take hyper thread into account
  static CpuBinder& instance();

  // query specific thread's cpu binding
  static std::vector<size_t> bound(size_t tid);

  // spawn a new instance from this
  std::unique_ptr<CpuBinder> spawn(size_t ncores, Policy policy);

  // set affinity for target thread
  bool bind(uint64_t tid);

  // set affinity for a IOThreadPoolExecutor
  bool bind(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool);

  // set affinity for a thrift PriorityThreadManager
  bool bind(std::shared_ptr<PriorityThreadManager> threadManager,
            const std::array<size_t, PRIORITY::N_PRIORITIES>& counts);

  // return available cores number if hyper thread disabled
  // return available processors number if hyper thread enabled
  uint32_t availableNumber() {
    std::lock_guard<std::mutex> guard(lock_);
    return availables_.size();
  }

 private:
  CpuBinder();
  CpuBinder(std::vector<size_t> availables, Policy policy);
  CpuBinder(const CpuBinder&) = delete;
  CpuBinder(CpuBinder&&) = delete;
  CpuBinder& operator=(const CpuBinder&) = delete;
  CpuBinder& operator=(CpuBinder&&) = delete;

  class DummyRunnable : public apache::thrift::concurrency::Runnable {
   public:
    DummyRunnable(folly::Synchronized<std::vector<uint64_t>, std::mutex>* tids,
                  folly::Baton<true, std::atomic>* baton,
                  size_t total)
        : tids_(tids), baton_(baton), total_(total) {
      CHECK_NOTNULL(tids);
      CHECK_NOTNULL(baton);
    }

    void run() {
      // sleep for a while to make sure tasks are distributed to all workers
      sleep(1);
      tids_->withLock([this](auto& locked) {
        locked.emplace_back(folly::getCurrentThreadID());
        if (locked.size() == total_) {
          baton_->post();
        }
      });
    }

   private:
    folly::Synchronized<std::vector<uint64_t>, std::mutex>* tids_;
    folly::Baton<true, std::atomic>* baton_;
    size_t total_;
  };

 private:
  std::mutex lock_;
  std::vector<size_t> availables_;
  Policy policy_{kExclusive};
};

}  // namespace cpu
}  // namespace nebula
