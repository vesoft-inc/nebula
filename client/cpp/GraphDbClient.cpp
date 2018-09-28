/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "client/cpp/GraphDbClient.h"
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include "dataman/RowSetReader.h"

DEFINE_int32(conn_timeout_ms, 1000,
             "Connection timeout in milliseconds");


namespace vesoft {
namespace vgraph {

enum class ClientError {
    SUCCEEDED = 0,

    E_DISCONNECTED = -1001,
    E_FAIL_TO_CONNECT = -1002,
    E_RPC_FAILURE = -1003,
};


GraphDbClient::GraphDbClient(const std::string& addr, uint16_t port)
        : addr_(addr)
        , port_(port)
        , sessionId_(0) {
}


GraphDbClient::~GraphDbClient() {
    disconnect();
}


int32_t GraphDbClient::connect(const std::string& username,
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
        errorStr_ = "Server is unavailable";
        return static_cast<int32_t>(ClientError::E_FAIL_TO_CONNECT);
    }

    // Wait until the socket bcomes connected
    for (int i = 0; i < 4; i++) {
        usleep(1000 * FLAGS_conn_timeout_ms / 4);
        if (socket->good()) {
            VLOG(2) << "Connected to " << addr_ << ":" << port_;
            break;
        }
    }
    if (!socket->good()) {
        LOG(ERROR) << "Timed out when connecting to " << addr_ << ":" << port_;
        errorStr_ = "Server is unavailable";
        return static_cast<int32_t>(ClientError::E_FAIL_TO_CONNECT);
    }

    client_ = std::make_unique<cpp2::GraphDbServiceAsyncClient>(
        HeaderClientChannel::newChannel(socket));

    cpp2::AuthResponse resp;
    try {
        client_->sync_authenticate(resp, username, password);
        if (resp.get_result() != cpp2::ResultCode::SUCCEEDED) {
            errorStr_ = std::move(*(resp.get_errorMsg()));
            return static_cast<int32_t>(resp.get_result());
        }
        sessionId_ = *(resp.get_sessionId());
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Thrift rpc call failed: " << ex.what();
        errorStr_ = folly::stringPrintf("Failed to make the RPC call: %s",
                                        ex.what());
        return static_cast<int32_t>(ClientError::E_RPC_FAILURE);
    }

    return static_cast<int32_t>(ClientError::SUCCEEDED);
}


void GraphDbClient::disconnect() {
    if (!client_) {
        return;
    }

    // Log out
    client_->sync_signout(sessionId_);

    client_.reset();
}


int32_t GraphDbClient::execute(folly::StringPiece stmt,
                               std::unique_ptr<RowSetReader>& rowsetReader) {
    if (!client_) {
        errorStr_ = "Disconnected from the server";
        return static_cast<int32_t>(ClientError::E_DISCONNECTED);
    }

    cpp2::ExecutionResponse resp;
    try {
        client_->sync_execute(resp, sessionId_, stmt.toString());
        if (resp.get_result() != cpp2::ResultCode::SUCCEEDED) {
            errorStr_ = std::move(*(resp.get_errorMsg()));
            return static_cast<int32_t>(resp.get_result());
        }
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Thrift rpc call failed: " << ex.what();
        errorStr_ = folly::stringPrintf("Failed to make the RPC call: %s",
                                        ex.what());
        return static_cast<int32_t>(ClientError::E_RPC_FAILURE);
    }

    latencyInMs_ = resp.get_latencyInMs();
    rowsetReader.reset(new RowSetReader(resp));
    return static_cast<int32_t>(ClientError::SUCCEEDED);
}


const char* GraphDbClient::getErrorStr() const {
    return errorStr_.c_str();
}


int32_t GraphDbClient::getServerLatency() const {
    return latencyInMs_;
}

}  // namespace vgraph
}  // namespace vesoft
