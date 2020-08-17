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

void NebulaClient::initConnectionPool(const std::vector<ConnectionInfo> &addrInfo) {
    graph::NebulaClientImpl::initConnectionPool(addrInfo);
}

NebulaClient::NebulaClient(const std::string &addr, const uint32_t port) {
    client_ = std::make_unique<graph::NebulaClientImpl>(addr, port);
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

void NebulaClient::asyncExecute(std::string stmt, ExecCallback cb, void *context) {
    auto doCallBack = [=] (ExecutionResponse *resp, ErrorCode code) {
        cb(resp, code, context);
    };
    return client_->doAsyncExecute(stmt, std::move(doCallBack));
}

}  // namespace nebula
