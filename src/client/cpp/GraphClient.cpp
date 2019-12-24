/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "client/cpp/GraphClient.h"
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


cpp2::ErrorCode GraphClient::connect(const std::string& username,
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

    cpp2::AuthResponse resp;
    try {
        client_->sync_authenticate(resp, username, password);
        if (resp.get_error_code() != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Failed to authenticate \"" << username << "\": "
                       << resp.get_error_msg();
            return resp.get_error_code();
        }
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Thrift rpc call failed: " << ex.what();
        return cpp2::ErrorCode::E_RPC_FAILURE;
    }

    sessionId_ = *(resp.get_session_id());
    return cpp2::ErrorCode::SUCCEEDED;
}


void GraphClient::disconnect() {
    if (!client_) {
        return;
    }

    // Log out
    client_->sync_signout(sessionId_);

    client_.reset();
}


cpp2::ErrorCode GraphClient::execute(folly::StringPiece stmt,
                                     cpp2::ExecutionResponse& resp) {
    if (!client_) {
        LOG(ERROR) << "Disconnected from the server";
        return cpp2::ErrorCode::E_DISCONNECTED;
    }

    try {
        client_->sync_execute(resp, sessionId_, stmt.toString());
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Thrift rpc call failed: " << ex.what();
        return cpp2::ErrorCode::E_RPC_FAILURE;
    }

    auto* msg = resp.get_error_msg();
    if (msg != nullptr) {
        LOG(WARNING) << *msg;
    }
    return resp.get_error_code();
}

}  // namespace graph
}  // namespace nebula
