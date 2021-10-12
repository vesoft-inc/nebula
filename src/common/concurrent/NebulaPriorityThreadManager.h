/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/concurrency/ThreadManager.h>

namespace nebula {
namespace concurrency {

class NebulaPriorityThreadManager : public apache::thrift::concurrency::PriorityThreadManager,
                                    public folly::DefaultKeepAliveExecutor {
 public:
  explicit NebulaPriorityThreadManager(size_t normalThreadsCount, bool enableTaskStats = false);

  void start() override { proxy_->start(); }

  void stop() override { proxy_->stop(); }

  void join() override { proxy_->join(); }

  std::string getNamePrefix() const override { return proxy_->getNamePrefix(); }

  void setNamePrefix(const std::string& name) override { proxy_->setNamePrefix(name); }

  void addWorker(size_t value) override { proxy_->addWorker(value); }

  void removeWorker(size_t value) override { proxy_->removeWorker(value); }

  void addWorker(PRIORITY priority, size_t value) override { proxy_->addWorker(priority, value); }

  void removeWorker(PRIORITY priority, size_t value) override {
    proxy_->removeWorker(priority, value);
  }

  size_t workerCount(PRIORITY priority) override { return proxy_->workerCount(priority); }

  STATE state() const override { return proxy_->state(); }

  std::shared_ptr<apache::thrift::concurrency::ThreadFactory> threadFactory() const override {
    return proxy_->threadFactory();
  }

  void threadFactory(std::shared_ptr<apache::thrift::concurrency::ThreadFactory> value) override {
    proxy_->threadFactory(value);
  }

  // using apache::thrift::concurrency::PriorityThreadManager::add;
  // using apache::thrift::concurrency::ThreadManager::add;
  void add(std::shared_ptr<apache::thrift::concurrency::Runnable> task,
           int64_t timeout,
           int64_t expiration,
           ThreadManager::Source source) noexcept override {
    proxy_->add(task, timeout, expiration, source);
  }

  void add(PRIORITY priority,
           std::shared_ptr<apache::thrift::concurrency::Runnable> task,
           int64_t timeout,
           int64_t expiration,
           ThreadManager::Source source) noexcept override {
    proxy_->add(priority, task, timeout, expiration, source);
  }

  KeepAlive<> getKeepAlive(ExecutionScope es, Source source) const override {
    return proxy_->getKeepAlive(es, source);
  }

  void add(folly::Func f) override { proxy_->add(std::move(f)); }

  void addWithPriority(folly::Func f, int8_t priority) override {
    proxy_->addWithPriority(std::move(f), priority);
  }

  size_t idleWorkerCount() const override { return proxy_->idleWorkerCount(); }

  size_t idleWorkerCount(PRIORITY priority) const override {
    return proxy_->idleWorkerCount(priority);
  }

  size_t workerCount() const override { return proxy_->workerCount(); }

  size_t pendingTaskCount() const override { return proxy_->pendingTaskCount(); }

  size_t pendingUpstreamTaskCount() const override { return proxy_->pendingUpstreamTaskCount(); }

  size_t pendingTaskCount(PRIORITY priority) const override {
    return proxy_->pendingTaskCount(priority);
  }

  size_t totalTaskCount() const override { return proxy_->totalTaskCount(); }

  size_t expiredTaskCount() override { return proxy_->expiredTaskCount(); }

  void remove(std::shared_ptr<apache::thrift::concurrency::Runnable> task) override {
    proxy_->remove(task);
  }

  std::shared_ptr<apache::thrift::concurrency::Runnable> removeNextPending() override {
    return proxy_->removeNextPending();
  }

  void clearPending() override { proxy_->clearPending(); }

  void setExpireCallback(ExpireCallback expireCallback) override {
    proxy_->setExpireCallback(std::move(expireCallback));
  }

  void setCodelCallback(ExpireCallback expireCallback) override {
    proxy_->setCodelCallback(std::move(expireCallback));
  }

  void setThreadInitCallback(InitCallback initCallback) override {
    proxy_->setThreadInitCallback(std::move(initCallback));
  }

  void enableCodel(bool enabled) override { proxy_->enableCodel(enabled); }

  folly::Codel* getCodel() override { return proxy_->getCodel(); }

  folly::Codel* getCodel(PRIORITY priority) override { return proxy_->getCodel(priority); }

 private:
  std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> proxy_;
};

}  // namespace concurrency
}  // namespace nebula
