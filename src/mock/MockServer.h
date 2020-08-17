/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_MOCKSERVER_H_
#define EXECUTOR_MOCKSERVER_H_

#include "common/base/Base.h"
#include "common/thread/NamedThread.h"
#include "mock/MockMetaServiceHandler.h"
#include "mock/MockStorageServiceHandler.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include "service/GraphService.h"

namespace nebula {
namespace graph {

class Server final {
public:
    ~Server() {
        if (server_ != nullptr) {
            server_->stop();
        }
        if (thread_ != nullptr) {
            thread_->join();
        }
        server_ = nullptr;
        thread_ = nullptr;
    }


    void mock(const std::string& name,
              std::shared_ptr<apache::thrift::ServerInterface> handler) {
        server_ = std::make_unique<apache::thrift::ThriftServer>();
        server_->setInterface(std::move(handler));
        server_->setPort(0);
        thread_ = std::make_unique<thread::NamedThread>(name, [this, name] {
            server_->serve();
            LOG(INFO) << "The " << name << " server has been stopped";
        });

        while (!server_->getServeEventBase() ||
               !server_->getServeEventBase()->isRunning()) {
            usleep(10000);
        }
        port_ = server_->getAddress().getPort();
    }

    uint16_t getPort() {
        return port_;
    }

private:
    std::unique_ptr<apache::thrift::ThriftServer>      server_{nullptr};
    std::unique_ptr<thread::NamedThread>               thread_{nullptr};
    uint16_t                                           port_{0};
};

class MockServer final {
public:
    MockServer() = default;
    ~MockServer() {
        graphServer_.reset();
        metaServer_.reset();
        storageServer_.reset();
    }

    uint16_t startMeta() {
        auto handler = std::make_shared<MockMetaServiceHandler>();
        metaServer_ = std::make_unique<Server>();
        metaServer_->mock("mock_meta", handler);
        LOG(INFO) << "Start meta service port: " << metaServer_->getPort();
        return metaServer_->getPort();
    }

    uint16_t startStorage(uint16_t metaPort) {
        auto handler = std::make_shared<MockStorageServiceHandler>(metaPort);
        storageServer_ = std::make_unique<Server>();
        storageServer_->mock("mock_storage", handler);
        LOG(INFO) << "Start storage service port: " << storageServer_->getPort();
        return storageServer_->getPort();
    }

    uint16_t startGraph() {
        auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
        auto handler = std::make_shared<GraphService>();
        graphServer_ = std::make_unique<Server>();
        auto status = handler->init(threadPool);
        CHECK(status.ok()) << status;
        graphServer_->mock("mock_graph", handler);
        LOG(INFO) << "Start graph service port: " << graphServer_->getPort();
        return graphServer_->getPort();
    }

private:
    std::unique_ptr<Server>           graphServer_;
    std::unique_ptr<Server>           metaServer_;
    std::unique_ptr<Server>           storageServer_;
};

}   // namespace graph
}   // namespace nebula
#endif  // EXECUTOR_MOCKSERVER_H_
