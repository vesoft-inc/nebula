/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "NebulaClientImpl.h"
#include "base/Base.h"

#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

DEFINE_int32(server_conn_timeout_ms, 1000,
             "Connection timeout in milliseconds");


namespace nebula {
namespace graph {

void NebulaClientImpl::initEnv(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
}

NebulaClientImpl::NebulaClientImpl(const std::string& addr, uint16_t port)
        : addr_(addr)
        , port_(port)
        , sessionId_(0) {
}


NebulaClientImpl::~NebulaClientImpl() {
    disconnect();
}

cpp2::ErrorCode NebulaClientImpl::connect(const std::string& username,
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

ErrorCode NebulaClientImpl::doConnect(const std::string& username,
                                      const std::string& password) {
    cpp2::ErrorCode code = connect(username, password);
    return static_cast<ErrorCode>(code);
}


void NebulaClientImpl::disconnect() {
    if (!client_) {
        return;
    }

    // Log out
    client_->sync_signout(sessionId_);
    client_.reset();
}

void NebulaClientImpl::feedRows(const cpp2::ExecutionResponse& inResp, ExecuteResponse& outResp) {
    std::vector<RowValue> rows;
    for (auto& row : (*inResp.get_rows())) {
        std::vector<ColValue> columns;
        for (auto &col : row.get_columns()) {
            columns.emplace_back();
            switch (col.getType()) {
                case cpp2::ColumnValue::Type::__EMPTY__: {
                    break;
                }
                case cpp2::ColumnValue::Type::bool_val: {
                    columns.back().setBoolValue(col.get_bool_val());
                    break;
                }
                case cpp2::ColumnValue::Type::integer: {
                    columns.back().setIntValue(col.get_integer());
                    break;
                }
                case cpp2::ColumnValue::Type::id: {
                    columns.back().setIdValue(col.get_id());
                    break;
                }
                case cpp2::ColumnValue::Type::single_precision: {
                    columns.back().setFloatValue(col.get_single_precision());
                    break;
                }
                case cpp2::ColumnValue::Type::double_precision: {
                    columns.back().setDoubleValue(col.get_double_precision());
                    break;
                }
                case cpp2::ColumnValue::Type::str: {
                    columns.back().setStrValue(std::move(col.get_str()));
                    break;
                }
                case cpp2::ColumnValue::Type::timestamp: {
                    columns.back().setTimestampValue(col.get_timestamp());
                    break;
                }
                case cpp2::ColumnValue::Type::year: {
                    break;
                }
                case cpp2::ColumnValue::Type::month: {
                    break;
                }
                case cpp2::ColumnValue::Type::date: {
                    break;
                }
                case cpp2::ColumnValue::Type::datetime: {
                    break;
                }
            }
        }
        rows.emplace_back();
        rows.back().setColumns(std::move(columns));
    }
    outResp.setRows(std::move(rows));
}

cpp2::ErrorCode NebulaClientImpl::execute(folly::StringPiece stmt,
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

ErrorCode NebulaClientImpl::doExecute(std::string stmt,
                                      ExecuteResponse& resp) {
    cpp2::ExecutionResponse response;
    cpp2::ErrorCode code = execute(stmt, response);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return static_cast<ErrorCode>(code);
    }

    resp.setErrorCode(static_cast<ErrorCode>(response.get_error_code()));
    resp.setLatencyInUs(response.get_latency_in_us());
    if (response.get_error_msg() != nullptr) {
        resp.setErrorMsg(std::move(*response.get_error_msg()));
    }
    if (response.get_space_name() != nullptr) {
        resp.setSpaceName(std::move(*response.get_space_name()));
    }
    if (response.get_column_names() != nullptr) {
        resp.setColumnNames(std::move(*response.get_column_names()));
    }
    if (response.get_rows() != nullptr) {
        feedRows(response, resp);
    }


    return static_cast<ErrorCode>(response.get_error_code());
}

}  // namespace graph
}  // namespace nebula
