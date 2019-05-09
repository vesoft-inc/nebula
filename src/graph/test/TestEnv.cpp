/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"

DECLARE_int32(load_data_interval_second);

namespace nebula {
namespace graph {


TestEnv *gEnv = nullptr;

TestEnv::TestEnv() {
}


TestEnv::~TestEnv() {
}


void TestEnv::SetUp() {
    FLAGS_load_data_interval_second = 1;
    using ThriftServer = apache::thrift::ThriftServer;
    server_ = std::make_unique<ThriftServer>();
    server_->getIOThreadPool()->setNumThreads(1);
    auto interface = std::make_shared<GraphService>(server_->getIOThreadPool());
    server_->setInterface(std::move(interface));
    server_->setPort(0);    // Let the system choose an available port for us
    auto serve = [this] {
        server_->serve();
    };

    thread_ = std::make_unique<thread::NamedThread>("", serve);
    // busy waiting for `thread_' to enter the loop
    while (!server_->getServeEventBase() || !server_->getServeEventBase()->isRunning()) {
    }
}


void TestEnv::TearDown() {
    if (server_ != nullptr) {
        server_->stop();
    }
    if (thread_ != nullptr) {
        thread_->join();
    }
    server_ = nullptr;
    thread_ = nullptr;
}


uint16_t TestEnv::serverPort() const {
    return server_->getAddress().getPort();
}


std::unique_ptr<GraphClient> TestEnv::getClient() const {
    auto client = std::make_unique<GraphClient>("127.0.0.1", serverPort());
    if (cpp2::ErrorCode::SUCCEEDED != client->connect("user", "password")) {
        return nullptr;
    }
    return client;
}

}   // namespace graph
}   // namespace nebula
