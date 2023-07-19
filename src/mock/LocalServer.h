/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef MOCK_LOCALSERVER_H
#define MOCK_LOCALSERVER_H

#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "common/base/Base.h"
#include "common/thread/NamedThread.h"
#include "storage/GraphStorageLocalServer.h"

namespace nebula {
namespace mock {

struct LocalServer {
  ~LocalServer() {
    VLOG(3) << "~LocalServer";
  }

  void start(const std::string& name,
             uint16_t port,
             std::shared_ptr<apache::thrift::ServerInterface> handler) {
    UNUSED(name);
    UNUSED(port);
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers =
        apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1);
    workers->setNamePrefix("executor");
    workers->start();

    server_ = storage::GraphStorageLocalServer::getInstance();
    server_->setThreadManager(workers);
    server_->setInterface(std::move(handler));
    usleep(10000);
  }

  std::shared_ptr<storage::GraphStorageLocalServer> server_{nullptr};
  uint16_t port_{0};
};

}  // namespace mock
}  // namespace nebula
#endif
