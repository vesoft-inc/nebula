/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_TEST_SERVERCONTEXT_H_
#define COMMON_TEST_SERVERCONTEXT_H_

#include "common/base/Base.h"
#include "common/thread/NamedThread.h"
#include "common/kvstore/KVStore.h"
#include "common/meta/SchemaManager.h"
#include "common/meta/IndexManager.h"
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
        kvStore_ = nullptr;
        schemaMan_.reset();
        indexMan_.reset();
        server_ = nullptr;
        thread_ = nullptr;
        VLOG(3) << "~ServerContext";
    }


    void mockCommon(const std::string& name,
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
    // To keep meta and storage's KVStore
    std::unique_ptr<kvstore::KVStore>                  kvStore_{nullptr};
    std::unique_ptr<meta::SchemaManager>               schemaMan_{nullptr};
    std::unique_ptr<meta::IndexManager>                indexMan_{nullptr};
    uint16_t                                           port_{0};
};


}   // namespace test
}   // namespace nebula
#endif  // COMMON_TEST_SERVERCONTEXT_H_
