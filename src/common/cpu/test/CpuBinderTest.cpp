/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/futures/Future.h>
#include <gtest/gtest.h>

#include "common/cpu/CpuBinder.h"
#include "common/cpu/CpuInfo.h"
#include "common/thread/GenericThreadPool.h"

namespace nebula {
namespace cpu {

TEST(CpuBinderTest, Spawn) {
  CpuInfo info;
  auto ncores = info.numOfCores();
  auto nproc = info.numOfProcessors();
  LOG(INFO) << folly::sformat("There are {} cores, {} processors", ncores, nproc);
  {
    CpuBinder binder;
    ASSERT_EQ(nullptr, binder.spawn(0, CpuBinder::kExclusive));
    ASSERT_EQ(nullptr, binder.spawn(nproc + 1, CpuBinder::kExclusive));
    ASSERT_NE(nullptr, binder.spawn(nproc, CpuBinder::kExclusive));
  }
  {
    CpuBinder binder;
    for (auto i = 0UL; i < nproc; i++) {
      ASSERT_NE(nullptr, binder.spawn(1UL, CpuBinder::kExclusive));
    }
    ASSERT_EQ(nullptr, binder.spawn(1UL, CpuBinder::kExclusive));
  }
}

TEST(CpuBinderTest, StdThreadExclusiveBindBySelf) {
  CpuBinder binder;
  CpuInfo info;
  std::thread thread([&]() {
    {
      auto bound = binder.bound(0);
      ASSERT_EQ(info.numOfProcessors(), bound.size());
    }
    {
      ASSERT_TRUE(binder.bind(0));
      ASSERT_EQ(1UL, binder.bound(0).size());
    }
  });
  thread.join();
}

TEST(CpuBinderTest, StdThreadExclusiveBindByOthers) {
  CpuBinder binder;
  std::atomic<bool> stop{false};
  std::thread thread([&]() {
    while (!stop.load()) {
    }
  });
  ASSERT_TRUE(binder.bind(thread.native_handle()));
  ASSERT_EQ(1UL, binder.bound(thread.native_handle()).size());
  stop.store(true);
  thread.join();
}

TEST(CpuBinderTest, StdThreadShareBindBySelf) {
  CpuInfo info;
  auto nproc = info.numOfProcessors() / 4UL;
  auto binder = CpuBinder().spawn(nproc, CpuBinder::kShared);
  std::thread thread([&]() {
    {
      ASSERT_TRUE(binder->bind(0));
      ASSERT_EQ(nproc, binder->bound(0).size());
    }
  });
  thread.join();
}

TEST(CpuBinderTest, StdThreadShareBindByOthers) {
  CpuInfo info;
  auto nproc = info.numOfProcessors() / 4UL;
  auto binder = CpuBinder().spawn(nproc, CpuBinder::kShared);
  std::atomic<bool> stop{false};
  std::thread thread([&]() {
    while (!stop.load()) {
    }
  });
  ASSERT_TRUE(binder->bind(thread.native_handle()));
  ASSERT_EQ(nproc, binder->bound(thread.native_handle()).size());
  stop.store(true);
  thread.join();
}

TEST(CpuBinderTest, GenericPoolExclusiveBindByOthers) {
  CpuInfo info;
  CpuBinder binder;
  auto nproc = info.numOfProcessors();
  thread::GenericThreadPool pool;
  pool.start(nproc);
  auto getTid = [] { return folly::getCurrentThreadID(); };
  auto getBound = [] { return CpuBinder::bound(0); };
  std::vector<folly::SemiFuture<uint64_t>> tids;
  std::vector<folly::SemiFuture<std::vector<size_t>>> bounds;
  for (auto i = 0UL; i < nproc; i++) {
    tids.emplace_back(pool.addTask(getTid));
  }
  for (auto&& fut : tids) {
    auto tid = std::move(fut).get();
    ASSERT_NE(0UL, tid);
    binder.bind(tid);
  }
  for (auto i = 0UL; i < nproc; i++) {
    bounds.emplace_back(pool.addTask(getBound));
  }
  for (auto&& fut : bounds) {
    ASSERT_EQ(1UL, std::move(fut).get().size());
  }
}

TEST(CpuBinderTest, GenericPoolShareBindByOthers) {
  CpuInfo info;
  auto nproc = info.numOfProcessors();
  nproc /= 4;
  auto binder = CpuBinder().spawn(nproc, CpuBinder::kShared);
  thread::GenericThreadPool pool;
  pool.start(nproc);
  auto getTid = [] { return folly::getCurrentThreadID(); };
  auto getBound = [] { return CpuBinder::bound(0); };
  std::vector<folly::SemiFuture<uint64_t>> tids;
  std::vector<folly::SemiFuture<std::vector<size_t>>> bounds;
  for (auto i = 0UL; i < nproc; i++) {
    tids.emplace_back(pool.addTask(getTid));
  }
  for (auto&& fut : tids) {
    auto tid = std::move(fut).get();
    ASSERT_NE(0UL, tid);
    binder->bind(tid);
  }
  for (auto i = 0UL; i < nproc; i++) {
    bounds.emplace_back(pool.addTask(getBound));
  }
  for (auto&& fut : bounds) {
    ASSERT_EQ(nproc, std::move(fut).get().size());
  }
}

TEST(CpuBinderTest, IOThreadPoolExclusiveBindByOthers) {
  CpuInfo info;
  auto ncores = info.numOfProcessors();
  CpuBinder binder;
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(ncores);
  EXPECT_TRUE(binder.bind(ioThreadPool));
}

TEST(CpuBinderTest, IOThreadPoolShareBindByOthers) {
  CpuInfo info;
  auto nproc = info.numOfProcessors();
  nproc /= 4;
  auto binder = CpuBinder().spawn(nproc, CpuBinder::kShared);
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(nproc);
  EXPECT_TRUE(binder->bind(ioThreadPool));
}

TEST(CpuBinderTest, ThreadManagerExclusiveBindByOthers) {
  using PriorityThreadManager = apache::thrift::concurrency::PriorityThreadManager;
  using PRIORITY = apache::thrift::concurrency::PRIORITY;

  CpuInfo info;
  auto nproc = info.numOfProcessors();
  const std::array<size_t, PRIORITY::N_PRIORITIES> counts{{0, 0, 0, nproc, 0}};
  auto pool = PriorityThreadManager::newPriorityThreadManager(counts, false);
  pool->start();

  CpuBinder binder;
  EXPECT_TRUE(binder.bind(pool, counts));
}

TEST(CpuBinderTest, ThreadManagerShareBindByOthers) {
  using PriorityThreadManager = apache::thrift::concurrency::PriorityThreadManager;
  using PRIORITY = apache::thrift::concurrency::PRIORITY;

  CpuInfo info;
  auto nproc = info.numOfProcessors();
  nproc /= 4;
  const std::array<size_t, PRIORITY::N_PRIORITIES> counts{{0, 0, 0, nproc, 0}};
  auto pool = PriorityThreadManager::newPriorityThreadManager(counts, false);
  pool->start();

  auto binder = CpuBinder().spawn(nproc, CpuBinder::kShared);
  EXPECT_TRUE(binder->bind(pool, counts));
}

}  // namespace cpu
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
