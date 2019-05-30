/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_TEST_SERVERCONTEXT_H_
#define COMMON_TEST_SERVERCONTEXT_H_

#include "base/Base.h"
#include "thread/NamedThread.h"
#include "kvstore/KVStore.h"
#include "meta/client/MetaClient.h"
#include <thrift/lib/cpp2/server/ThriftServer.h>

namespace nebula {
namespace test {

struct ServerContext {
    ~ServerContext() {
        if (server_ != nullptr) {
            server_->stop();
        }
        if (thread_ != nullptr) {
            thread_->join();
        }
        KVStore_ = nullptr;
        server_ = nullptr;
        thread_ = nullptr;
        VLOG(3) << "~ServerContext";
    }

    std::unique_ptr<apache::thrift::ThriftServer>      server_{nullptr};
    std::unique_ptr<thread::NamedThread>               thread_{nullptr};
    // To keep meta and storage's KVStore
    std::unique_ptr<kvstore::KVStore>                  KVStore_{nullptr};
    uint16_t                                           port_{0};
};

static void mockCommon(test::ServerContext *sc,
                       const std::string &name,
                       uint16_t port,
                       std::shared_ptr<apache::thrift::ServerInterface> handler) {
    if (nullptr == sc) {
        LOG(ERROR) << "ServerContext is nullptr";
        return;
    }
    sc->server_ = std::make_unique<apache::thrift::ThriftServer>();
    sc->server_->setInterface(std::move(handler));
    sc->server_->setPort(port);
    sc->thread_ = std::make_unique<thread::NamedThread>(name, [&]() {
        sc->server_->serve();
        LOG(INFO) << "Stop the server...";
    });
    while (!sc->server_->getServeEventBase() ||
           !sc->server_->getServeEventBase()->isRunning()) {
    }
    sc->port_ = sc->server_->getAddress().getPort();
}

}   // namespace test
}   // namespace nebula
#endif  // COMMON_TEST_SERVERCONTEXT_H_
