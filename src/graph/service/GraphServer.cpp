/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "GraphServer.h"

#include <folly/executors/IOThreadPoolExecutor.h>               // for IOThr...
#include <folly/executors/thread_factory/NamedThreadFactory.h>  // for Named...
#include <thrift/lib/cpp/concurrency/ThreadManager.h>           // for Threa...
#include <thrift/lib/cpp2/server/Cpp2ConnContext.h>             // for Prior...
#include <thrift/lib/cpp2/server/ThriftServer.h>                // for Thrif...

#include <chrono>     // for micro...
#include <exception>  // for excep...
#include <memory>     // for uniqu...
#include <ostream>    // for opera...
#include <string>     // for string
#include <thread>     // for sleep...
#include <utility>    // for move

#include "common/base/Base.h"            // for FLOG_...
#include "common/base/Logging.h"         // for LogMe...
#include "common/base/Status.h"          // for opera...
#include "common/id/Snowflake.h"         // for Snowf...
#include "common/ssl/SSLConfig.h"        // for sslCo...
#include "graph/service/GraphFlags.h"    // for FLAGS...
#include "graph/service/GraphService.h"  // for Graph...

namespace nebula {
namespace graph {

GraphServer::GraphServer(HostAddr localHost) : localHost_(std::move(localHost)) {}

GraphServer::~GraphServer() {
  stop();
}

bool GraphServer::start() {
  auto threadFactory = std::make_shared<folly::NamedThreadFactory>("graph-netio");
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_num_netio_threads,
                                                                    std::move(threadFactory));
  int numThreads = FLAGS_num_worker_threads > 0 ? FLAGS_num_worker_threads
                                                : thriftServer_->getNumIOWorkerThreads();
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      PriorityThreadManager::newPriorityThreadManager(numThreads));
  threadManager->setNamePrefix("executor");
  threadManager->start();

  thriftServer_ = std::make_unique<apache::thrift::ThriftServer>();
  thriftServer_->setIOThreadPool(ioThreadPool);

  auto interface = std::make_shared<GraphService>();
  auto status = interface->init(ioThreadPool, localHost_);
  if (!status.ok()) {
    LOG(ERROR) << status;
    return false;
  }

  nebula::Snowflake::initWorkerId(interface->metaClient_.get());

  graphThread_ = std::make_unique<std::thread>([&] {
    thriftServer_->setPort(localHost_.port);
    thriftServer_->setInterface(std::move(interface));
    thriftServer_->setReusePort(FLAGS_reuse_port);
    thriftServer_->setIdleTimeout(std::chrono::seconds(FLAGS_client_idle_timeout_secs));
    thriftServer_->setNumAcceptThreads(FLAGS_num_accept_threads);
    thriftServer_->setListenBacklog(FLAGS_listen_backlog);
    if (FLAGS_enable_ssl || FLAGS_enable_graph_ssl) {
      thriftServer_->setSSLConfig(nebula::sslContextConfig());
    }
    thriftServer_->setThreadManager(threadManager);

    serverStatus_.store(STATUS_RUNNING);
    FLOG_INFO("Starting nebula-graphd on %s:%d\n", localHost_.host.c_str(), localHost_.port);
    try {
      thriftServer_->serve();  // Blocking wait until shut down via thriftServer_->stop()
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
    cvStop_.wait(lkStop, [&] { return serverStatus_ != STATUS_RUNNING; });
  }

  thriftServer_->stop();

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

  if (thriftServer_) {
    thriftServer_->stop();
  }
}

}  // namespace graph
}  // namespace nebula
