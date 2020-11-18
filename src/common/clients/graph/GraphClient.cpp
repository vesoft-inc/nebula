/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/clients/graph/GraphClient.h"
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

DEFINE_int32(server_conn_timeout_ms, 1000,
             "Connection timeout in milliseconds");


namespace nebula {
namespace graph {

GraphClient::GraphClient(const std::string& addr, uint16_t port)
        : addr_(addr)
        , port_(port)
        , sessionId_(0) {
}


GraphClient::~GraphClient() {
    disconnect();
}


nebula::ErrorCode GraphClient::connect(const std::string& username,
                                       const std::string& password) {
    using apache::thrift::async::TAsyncSocket;
    using apache::thrift::HeaderClientChannel;

    auto socket = TAsyncSocket::newSocket(
        folly::EventBaseManager::get()->getEventBase(),
        addr_,
        port_,
        FLAGS_server_conn_timeout_ms);

    client_ = std::make_unique<cpp2::GraphServiceAsyncClient>(
        HeaderClientChannel::newChannel(socket));

    nebula::AuthResponse resp;
    try {
        client_->sync_authenticate(resp, username, password);
        if (resp.errorCode != nebula::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Failed to authenticate \"" << username << "\": ";
// TODO(sye) In order to support multi-language, a separate module will e provided
//           for looking up the error messages
//                       << resp.get_error_msg();
            return resp.errorCode;
        }
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Thrift rpc call failed: " << ex.what();
        return nebula::ErrorCode::E_RPC_FAILURE;
    }

    sessionId_ = *resp.sessionId;
    return nebula::ErrorCode::SUCCEEDED;
}


void GraphClient::disconnect() {
    if (!client_) {
        return;
    }

    // Log out
    client_->sync_signout(sessionId_);

    client_.reset();
}


nebula::ErrorCode GraphClient::execute(folly::StringPiece stmt,
                                       nebula::ExecutionResponse& resp) {
    if (!client_) {
        LOG(ERROR) << "Disconnected from the server";
        return nebula::ErrorCode::E_DISCONNECTED;
    }

    try {
        client_->sync_execute(resp, sessionId_, stmt.toString());
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Thrift rpc call failed: " << ex.what();
        return nebula::ErrorCode::E_RPC_FAILURE;
    }

    return resp.errorCode;
}

}  // namespace graph
}  // namespace nebula
