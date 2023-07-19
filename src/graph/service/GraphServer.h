/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <cstdint>
#include <mutex>
#include <thread>

#include "common/base/Base.h"
#include "common/base/SignalHandler.h"
#include "common/network/NetworkUtils.h"
namespace nebula {
namespace graph {
class GraphServer {
 public:
  explicit GraphServer(HostAddr localHost);

  ~GraphServer();

  // Return false if failed.
  bool start();

  void stop();

  // Used for signal handler to set an internal stop flag
  void notifyStop();

  void waitUntilStop();

 private:
  HostAddr localHost_;

  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers_;
  std::unique_ptr<apache::thrift::ThriftServer> thriftServer_;
  std::unique_ptr<std::thread> graphThread_;

  enum ServiceStatus : uint8_t { STATUS_UNINITIALIZED = 0, STATUS_RUNNING = 1, STATUS_STOPPED = 2 };
  std::atomic<ServiceStatus> serverStatus_{STATUS_UNINITIALIZED};
  std::mutex muStop_;
  std::condition_variable cvStop_;
};
}  // namespace graph
}  // namespace nebula
