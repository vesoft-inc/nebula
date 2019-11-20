/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "NebulaClientImpl.h"
#include "client/cpp/include/nebula/NebulaClient.h"

namespace nebula {

void NebulaClient::init(int argc, char *argv[]) {
    graph::NebulaClientImpl::initEnv(argc, argv);
}

void NebulaClient::initSocketPool(const std::string& addr,
                                  uint16_t port,
                                  int32_t timeout,
                                  int32_t socketNum) {
    graph::NebulaClientImpl::initSocketPool(addr, port, timeout, socketNum);
}

NebulaClient::NebulaClient() {
    client_ = std::make_unique<graph::NebulaClientImpl>();
}

NebulaClient::~NebulaClient() {
    client_->disconnect();
    client_ = nullptr;
}

ErrorCode NebulaClient::connect(const std::string& username,
                                const std::string& password) {
    return client_->doConnect(username, password);
}

void NebulaClient::disconnect() {
    client_->disconnect();
}

ErrorCode NebulaClient::execute(std::string stmt, ExecuteResponse& resp) {
    return client_->doExecute(stmt, resp);
}

void NebulaClient::asyncExecute(std::string stmt, CallbackFun cb) {
    return client_->doAsyncExecute(stmt, std::move(cb));
}

}  // namespace nebula
