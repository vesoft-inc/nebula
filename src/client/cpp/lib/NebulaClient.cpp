/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "NebulaClientImpl.h"
#include "client/cpp/include/nebula/NebulaClient.h"

namespace nebula {
namespace graph {

void NebulaClient::init(int argc, char *argv[]) {
    NebulaClientImpl::initEnv(argc, argv);
}

NebulaClient::NebulaClient(const std::string& addr, uint16_t port) {
    client_ = std::make_unique<NebulaClientImpl>(addr, port);
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

}  // namespace graph
}  // namespace nebula
