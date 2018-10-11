/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "client/cpp/GraphDbClient.h"
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

DEFINE_int32(conn_timeout_ms, 1000,
             "Connection timeout in milliseconds");


namespace vesoft {
namespace vgraph {

GraphDbClient::GraphDbClient(const std::string& addr, uint16_t port)
        : addr_(addr)
        , port_(port)
        , sessionId_(0) {
}


GraphDbClient::~GraphDbClient() {
    disconnect();
}


cpp2::ErrorCode GraphDbClient::connect(const std::string& username,
                                       const std::string& password) {
    using namespace apache::thrift;

    auto socket = async::TAsyncSocket::newSocket(
        folly::EventBaseManager::get()->getEventBase(),
        addr_,
        port_,
        FLAGS_conn_timeout_ms);
    if (!socket) {
        // Bad connection
        LOG(ERROR) << "Failed to connect to " << addr_ << ":" << port_;
        return cpp2::ErrorCode::E_FAIL_TO_CONNECT;
    }

    // Wait until the socket becomes connected
    // TODO Obviously this is not the most efficient way. We need to
    // change it to async implementation later
    for (int i = 0; i < 4; i++) {
        usleep(1000 * FLAGS_conn_timeout_ms / 4);
        if (socket->good()) {
            VLOG(2) << "Connected to " << addr_ << ":" << port_;
            break;
        }
    }
    if (!socket->good()) {
        LOG(ERROR) << "Timed out when connecting to " << addr_ << ":" << port_;
        return cpp2::ErrorCode::E_FAIL_TO_CONNECT;
    }

    client_ = std::make_unique<cpp2::GraphDbServiceAsyncClient>(
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


void GraphDbClient::disconnect() {
    if (!client_) {
        return;
    }

    // Log out
    client_->sync_signout(sessionId_);

    client_.reset();
}


cpp2::ErrorCode GraphDbClient::execute(folly::StringPiece stmt,
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

    return resp.get_error_code();
}

}  // namespace vgraph
}  // namespace vesoft
