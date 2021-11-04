/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/cpu/CpuBinder.h"

#include <folly/system/ThreadId.h>
#include <sched.h>

#include "common/cpu/CpuInfo.h"

namespace nebula {
namespace cpu {

CpuBinder& CpuBinder::instance() {
  static CpuBinder instance;
  return instance;
}

CpuBinder::CpuBinder() {
  CpuInfo info;
  size_t num = info.numOfProcessors();
  if (num > CPU_SETSIZE) {
    // cpu_set_t supports up to 1024 cores
    throw std::runtime_error("Too many cores");
  }
  availables_.resize(num);
  std::iota(availables_.begin(), availables_.end(), 0UL);
}

CpuBinder::CpuBinder(std::vector<size_t> availables, Policy policy) {
  policy_ = policy;
  availables_ = std::move(availables);
}

std::unique_ptr<CpuBinder> CpuBinder::spawn(size_t ncores, Policy policy) {
  if (ncores == 0) {
    LOG(ERROR) << "Requested number of cores cannot be zero";
    return nullptr;
  }

  std::unique_ptr<CpuBinder> binder;
  {
    std::lock_guard<std::mutex> guard(lock_);
    if (ncores > availables_.size()) {
      LOG(ERROR) << "Available cores not enough, " << ncores << " requested, " << availables_.size()
                 << " left";
      return nullptr;
    }
    // spawn ncores from this, return the spawned one CpuBinder
    auto end = availables_.end();
    auto iter = end;
    std::advance(iter, -ncores);
    std::vector<size_t> cores;
    cores.reserve(ncores);
    while (iter != end) {
      cores.push_back(*iter++);
    }
    availables_.resize(availables_.size() - ncores);
    binder = std::unique_ptr<CpuBinder>(new CpuBinder(std::move(cores), policy));
  }
  return binder;
}

bool CpuBinder::bind(uint64_t tid) {
  if (tid == 0) {
    tid = folly::getCurrentThreadID();
  }
  cpu_set_t set;
  CPU_ZERO(&set);

  // get cpu set according to policy
  {
    std::lock_guard<std::mutex> guard(lock_);
    if (availables_.empty()) {
      LOG(ERROR) << "No available cores";
      return false;
    }

    if (policy_ == kExclusive) {
      auto cpu = availables_.back();
      availables_.pop_back();
      CPU_SET(cpu, &set);
    } else if (policy_ == kShared) {
      for (auto cpu : availables_) {
        CPU_SET(cpu, &set);
      }
    }
  }

  // set thread affinity
  auto ok = pthread_setaffinity_np(static_cast<pthread_t>(tid), sizeof(set), &set) == 0;
  if (!ok) {
    LOG(ERROR) << folly::sformat(
        "Set CPU affinity to thread {} failled: {}", tid, ::strerror(errno));
  }
  return ok;
}

std::vector<size_t> CpuBinder::bound(size_t tid) {
  if (tid == 0) {
    tid = pthread_self();
  }
  cpu_set_t set;
  CPU_ZERO(&set);

  std::vector<size_t> bound;
  auto ok = pthread_getaffinity_np(static_cast<pthread_t>(tid), sizeof(set), &set) == 0;
  if (!ok) {
    LOG(ERROR) << "Get CPU affinity failed: " << ::strerror(errno);
    return bound;
  }

  auto cpu = 0;
  while (cpu < CPU_SETSIZE) {
    if (CPU_ISSET(cpu, &set)) {
      bound.emplace_back(cpu);
    }
    cpu++;
  }
  return bound;
}

bool CpuBinder::bind(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool) {
  size_t count = ioThreadPool->numThreads();
  std::vector<uint64_t> tids;
  tids.reserve(count);
  std::mutex lock;
  folly::Baton<true, std::atomic> baton;
  for (size_t i = 0; i < count; i++) {
    ioThreadPool->add([&] {
      std::lock_guard<std::mutex> lg(lock);
      tids.emplace_back(folly::getCurrentThreadID());
      if (tids.size() == count) {
        baton.post();
      }
    });
  }
  baton.wait();
  std::unordered_set<uint64_t> dedup(tids.begin(), tids.end());
  if (dedup.size() != count) {
    LOG(WARNING) << folly::sformat(
        "Failed to collect all io thread, skip cpu binding, expect {}, actual {}",
        count,
        dedup.size());
    return false;
  }
  for (const auto& tid : tids) {
    if (!bind(tid)) {
      return false;
    }
  }
  LOG(INFO) << folly::sformat("Bind {} cpu {} to {} successfully ",
                              count,
                              policy_ == kExclusive ? "exclusive" : "shared",
                              "IOThreadPool");
  return true;
}

bool CpuBinder::bind(std::shared_ptr<PriorityThreadManager> threadManager,
                     const std::array<size_t, PRIORITY::N_PRIORITIES>& counts) {
  folly::Synchronized<std::vector<uint64_t>, std::mutex> tids;
  folly::Baton<true, std::atomic> baton;
  size_t total = std::accumulate(counts.begin(), counts.end(), 0);
  for (int8_t pri = 0; pri < PRIORITY::N_PRIORITIES; pri++) {
    auto count = counts[pri];
    for (size_t i = 0; i < count; i++) {
      threadManager->add(static_cast<PRIORITY>(pri),
                         std::make_shared<DummyRunnable>(&tids, &baton, total));
    }
  }
  baton.wait();

  bool ret = true;
  tids.withLock([&](const auto& locked) {
    std::unordered_set<uint64_t> dedup(locked.begin(), locked.end());
    if (dedup.size() != total) {
      LOG(WARNING) << folly::sformat(
          "Failed to collect all worker thread, skip cpu binding, expect {}, actual {}",
          total,
          dedup.size());
      ret = false;
      return;
    }
    for (const auto& tid : locked) {
      if (!bind(tid)) {
        ret = false;
        return;
      }
    }
    LOG(INFO) << folly::sformat("Bind {} cpu {} to {} successfully ",
                                total,
                                policy_ == kExclusive ? "exclusive" : "shared",
                                threadManager->getNamePrefix());
  });
  return ret;
}

}  // namespace cpu
}  // namespace nebula
