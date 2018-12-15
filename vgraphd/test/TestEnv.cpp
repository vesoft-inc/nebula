/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/test/TestEnv.h"

namespace vesoft {
namespace vgraph {


TestEnv *gEnv = nullptr;

TestEnv::TestEnv() {
}


TestEnv::~TestEnv() {
}


void TestEnv::SetUp() {
    auto interface = std::make_shared<VGraphService>();
    using ThriftServer = apache::thrift::ThriftServer;
    server_ = std::make_unique<ThriftServer>();
    server_->setInterface(std::move(interface));
    server_->setPort(0);    // Let the system choose an available port for us

    auto serve = [this] {
        server_->serve();
    };

    thread_ = std::make_unique<thread::NamedThread>("", serve);
    // busy waiting for `thread_' to enter the loop
    while (!server_->getServeEventBase() || !server_->getServeEventBase()->isRunning()) {
        ;
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


std::unique_ptr<GraphDbClient> TestEnv::getClient() const {
    auto client = std::make_unique<GraphDbClient>("127.0.0.1", serverPort());
    if (cpp2::ErrorCode::SUCCEEDED != client->connect("user", "password")) {
        return nullptr;
    }
    return client;
}

}   // namespace vgraph
}   // namespace vesoft
