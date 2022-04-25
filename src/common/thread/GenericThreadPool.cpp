/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/thread/GenericThreadPool.h"

#include "common/base/Base.h"

namespace nebula {
namespace thread {

GenericThreadPool::GenericThreadPool() {}

GenericThreadPool::~GenericThreadPool() {
  stop();
  wait();
}

bool GenericThreadPool::start(size_t nrThreads, const std::string &name) {
  if (nrThreads_ != 0) {
    return false;
  }
  nrThreads_ = nrThreads;
  auto ok = true;
  for (auto i = 0UL; ok && i < nrThreads_; i++) {
    pool_.emplace_back(std::make_unique<GenericWorker>());
    auto workerName = folly::stringPrintf("%s-%lu", name.c_str(), i);
    ok = ok && pool_.back()->start(std::move(workerName));
  }
  return ok;
}

bool GenericThreadPool::stop() {
  auto ok = true;
  for (auto &worker : pool_) {
    ok = worker->stop() && ok;
  }
  return ok;
}

bool GenericThreadPool::wait() {
  auto ok = true;
  for (auto &worker : pool_) {
    ok = worker->wait() && ok;
  }
  nrThreads_ = 0;
  pool_.clear();
  return ok;
}

void GenericThreadPool::purgeTimerTask(uint64_t id) {
  auto idx = (id >> GenericWorker::TIMER_ID_BITS);
  id = (id & GenericWorker::TIMER_ID_MASK);
  pool_[idx]->purgeTimerTask(id);
}

}  // namespace thread
}  // namespace nebula
