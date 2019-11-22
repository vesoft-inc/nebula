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

void NebulaClient::initConnectionPool(const std::string& addr,
                                      uint16_t port,
                                      uint16_t connectionNum,
                                      int32_t timeout) {
    graph::NebulaClientImpl::initConnectionPool(addr, port, connectionNum, timeout);
}

NebulaClient::NebulaClient() {
    client_ = std::make_unique<graph::NebulaClientImpl>();
}

NebulaClient::~NebulaClient() {
    client_->signout();
    client_ = nullptr;
}

ErrorCode NebulaClient::authenticate(const std::string& username,
                                     const std::string& password) {
    return client_->doAuthenticate(username, password);
}

void NebulaClient::signout() {
    client_->signout();
}

ErrorCode NebulaClient::execute(std::string stmt, ExecutionResponse& resp) {
    return client_->doExecute(stmt, resp);
}

void NebulaClient::asyncExecute(std::string stmt, CallbackFun cb) {
    return client_->doAsyncExecute(stmt, std::move(cb));
}

}  // namespace nebula
