/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "GraphServer.h"

#include <memory>

#include "graph/service/GraphFlags.h"
#include "graph/service/GraphService.h"
namespace nebula {
namespace graph {

GraphServer::GraphServer(HostAddr localHost) : localHost_(localHost) {}

GraphServer::~GraphServer() {
  stop();
}

bool GraphServer::start() {
  auto threadFactory = std::make_shared<folly::NamedThreadFactory>("graph-netio");
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_netio_threads,
                                                                    std::move(threadFactory));
  int numThreads =
      FLAGS_num_worker_threads > 0 ? FLAGS_num_worker_threads : gServer_->getNumIOWorkerThreads();
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      PriorityThreadManager::newPriorityThreadManager(numThreads));
  threadManager->setNamePrefix("executor");
  threadManager->start();

  gServer_ = std::make_unique<apache::thrift::ThriftServer>();
  gServer_->setIOThreadPool(ioThreadPool);

  auto interface = std::make_shared<GraphService>();
  auto status = interface->init(ioThreadPool, localHost_);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return false;
  }

  graphThread_ = std::make_unique<std::thread>([&] {
    gServer_->setPort(localHost_.port);
    gServer_->setInterface(std::move(interface));
    gServer_->setReusePort(FLAGS_reuse_port);
    gServer_->setIdleTimeout(std::chrono::seconds(FLAGS_client_idle_timeout_secs));
    gServer_->setNumAcceptThreads(FLAGS_num_accept_threads);
    gServer_->setListenBacklog(FLAGS_listen_backlog);
    if (FLAGS_enable_ssl || FLAGS_enable_graph_ssl) {
      gServer_->setSSLConfig(nebula::sslContextConfig());
    }
    gServer_->setThreadManager(threadManager);

    serverStatus_.store(STATUS_RUNNING);
    FLOG_INFO("Starting nebula-graphd on %s:%d\n", localHost_.host.c_str(), localHost_.port);
    try {
      gServer_->serve();  // Blocking wait until shut down via gServer_->stop()
    } catch (const std::exception &e) {
      FLOG_ERROR("Exception thrown while starting the graph RPC server: %s", e.what());
    }
    serverStatus_.store(STATUS_STOPPED);
    FLOG_INFO("nebula-graphd on %s:%d has been stopped", localHost_.host.c_str(), localHost_.port);
  });

  while (serverStatus_ == STATUS_UNINITIALIZED) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  return true;
}

void GraphServer::waitUntilStop() {
  {
    std::unique_lock<std::mutex> lkStop(muStop_);
    while (serverStatus_ == STATUS_RUNNING) {
      cvStop_.wait(lkStop);
    }
  }

  gServer_->stop();

  graphThread_->join();
}

void GraphServer::notifyStop() {
  std::unique_lock<std::mutex> lkStop(muStop_);
  if (serverStatus_ == STATUS_RUNNING) {
    serverStatus_ = STATUS_STOPPED;
    cvStop_.notify_one();
  }
}

void GraphServer::stop() {
  if (serverStatus_.load() == ServiceStatus::STATUS_STOPPED) {
    LOG(INFO) << "The graph server has been stopped";
    return;
  }

  ServiceStatus serverExpected = ServiceStatus::STATUS_RUNNING;
  serverStatus_.compare_exchange_strong(serverExpected, STATUS_STOPPED);

  if (gServer_) {
    gServer_->stop();
  }
}

}  // namespace graph
}  // namespace nebula
