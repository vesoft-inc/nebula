/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_RPCSERVER_H_
#define MOCK_RPCSERVER_H_

#include "common/base/Base.h"
#include "common/thread/NamedThread.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>

namespace nebula {
namespace mock {

struct RpcServer {
    ~RpcServer() {
        if (server_ != nullptr) {
            server_->stop();
        }
        if (thread_ != nullptr) {
            thread_->join();
        }
        VLOG(3) << "~RpcServer";
    }


    void start(const std::string& name,
               uint16_t port,
               std::shared_ptr<apache::thrift::ServerInterface> handler) {
        server_ = std::make_unique<apache::thrift::ThriftServer>();
        server_->setInterface(std::move(handler));
        server_->setPort(port);
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


    std::unique_ptr<apache::thrift::ThriftServer>      server_{nullptr};
    std::unique_ptr<thread::NamedThread>               thread_{nullptr};
    uint16_t                                           port_{0};
};


}   // namespace mock
}   // namespace nebula
#endif  // MOCK_RPCSERVER_H_
