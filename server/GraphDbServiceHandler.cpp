/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "server/GraphDbServiceHandler.h"
#include "time/Duration.h"
#include "dataman/RowWriter.h"
#include "dataman/RowSetWriter.h"
#include "dataman/ThriftSchemaProvider.h"

namespace vesoft {
namespace vgraph {

folly::Future<cpp2::AuthResponse> GraphDbServiceHandler::future_authenticate(
        const std::string& username,
        const std::string& password) {
    VLOG(2) << "Authenticating user \"" << username << "\"";

    cpp2::AuthResponse resp;
    // TODO(sye) For test purpose, we only allow test/user to pass
    if (username == "test" && password == "user") {
        resp.set_result(cpp2::ResultCode::SUCCEEDED);
        resp.set_sessionId(1);
    } else {
        resp.set_result(cpp2::ResultCode::E_BAD_USERNAME_PASSWORD);
        resp.set_errorMsg(getErrorStr(cpp2::ResultCode::E_BAD_USERNAME_PASSWORD));
    }

    folly::Promise<cpp2::AuthResponse> promise;
    promise.setValue(std::move(resp));
    return promise.getFuture();
}


void GraphDbServiceHandler::signout(int64_t sessionId) {
    VLOG(2) << "Sign out the session " << sessionId;
}


folly::Future<cpp2::ExecutionResponse> GraphDbServiceHandler::future_execute(
        int64_t sessionId,
        const std::string& stmt) {
    VLOG(2) << "Executing statement \"" << stmt << "\" in session "
            << sessionId;

    cpp2::ExecutionResponse resp;
    time::Duration dur;

    // TODO For test purpose, we only generate two valid results and
    // return error for all others
    if (stmt == "version") {
        resp.set_result(cpp2::ResultCode::SUCCEEDED);

        RowWriter row1;
        row1 << RowWriter::ColName("major") << 1
             << RowWriter::ColName("minor") << 2
             << RowWriter::ColName("release") << 1234567890;
        resp.set_schema(std::move(row1.moveSchema()));

        ThriftSchemaProvider schema(resp.get_schema());

        RowSetWriter rs(&schema);
        rs.addRow(row1);

        RowWriter row2(&schema);
        row2 << 2 << 3 << 4;
        rs.addRow(row2);

        resp.set_data(std::move(rs.data()));
    } else if (stmt == "select") {
        usleep(2000);
        resp.set_result(cpp2::ResultCode::SUCCEEDED);
    } else {
        // TODO otherwuse, return error
        resp.set_result(cpp2::ResultCode::E_SYNTAX_ERROR);
        resp.set_errorMsg("Syntax error: Unknown statement");
    }

    resp.set_latencyInMs(dur.elapsedInMSec());

    folly::Promise<cpp2::ExecutionResponse> promise;
    promise.setValue(std::move(resp));
    return promise.getFuture();
}


const char* GraphDbServiceHandler::getErrorStr(cpp2::ResultCode result) {
    switch (result) {
    case cpp2::ResultCode::SUCCEEDED:
        return "Succeeded";
    /**********************
     * Server side errors
     **********************/
    case cpp2::ResultCode::E_BAD_USERNAME_PASSWORD:
        return "Bad username/password";
    case cpp2::ResultCode::E_SESSION_INVALID:
        return "The session is invalid";
    case cpp2::ResultCode::E_SESSION_TIMEOUT:
        return "The session timed out";
    case cpp2::ResultCode::E_SYNTAX_ERROR:
        return "Syntax error";
    /**********************
     * Unknown error
     **********************/
    default:
        return "Unknown error";
    }
}
}  // namespace vgraph
}  // namespace vesoft

