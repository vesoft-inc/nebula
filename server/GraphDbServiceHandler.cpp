/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "server/GraphDbServiceHandler.h"
#include "time/Duration.h"

namespace vesoft {
namespace vgraph {

folly::Future<cpp2::AuthResponse> GraphDbServiceHandler::future_authenticate(
        const std::string& username,
        const std::string& password) {
    VLOG(2) << "Authenticating user \"" << username << "\"";

    cpp2::AuthResponse resp;
    // TODO(sye) For test purpose, we only allow test/user to pass
    if (username == "test" && password == "user") {
        resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
        resp.set_session_id(1);
    } else {
        resp.set_error_code(cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD);
        resp.set_error_msg(getErrorStr(cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD));
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
        resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);

        std::vector<std::string> headers;
        headers.emplace_back("major");
        headers.emplace_back("minor");
        headers.emplace_back("release");
        headers.emplace_back("release_date");
        headers.emplace_back("expiration");
        resp.set_column_names(std::move(headers));

        std::vector<cpp2::ColumnValue> cols;

        // Row 1
        cols.emplace_back(cpp2::ColumnValue());
        cols.back().set_integer(1);
        cols.emplace_back(cpp2::ColumnValue());
        cols.back().set_integer(1);
        cols.emplace_back(cpp2::ColumnValue());
        cols.back().set_integer(1);
        cols.emplace_back(cpp2::ColumnValue());
        cpp2::Date date;
        date.year = 2017;
        date.month = 10;
        date.day = 8;
        cols.back().set_date(std::move(date));
        cols.emplace_back(cpp2::ColumnValue());
        cpp2::YearMonth month;
        month.year = 2018;
        month.month = 4;
        cols.back().set_month(std::move(month));

        std::vector<cpp2::RowValue> rows;
        rows.emplace_back();
        rows.back().set_columns(std::move(cols));

        // Row 2
        cols.clear();
        cols.emplace_back(cpp2::ColumnValue());
        cols.back().set_integer(2);
        cols.emplace_back(cpp2::ColumnValue());
        cols.back().set_integer(2);
        cols.emplace_back(cpp2::ColumnValue());
        cols.back().set_integer(2);
        cols.emplace_back(cpp2::ColumnValue());
        date.year = 2018;
        date.month = 10;
        date.day = 8;
        cols.back().set_date(std::move(date));
        cols.emplace_back(cpp2::ColumnValue());
        month.year = 2019;
        month.month = 4;
        cols.back().set_month(std::move(month));

        rows.emplace_back();
        rows.back().set_columns(std::move(cols));

        resp.set_rows(std::move(rows));
    } else if (stmt == "select") {
        usleep(2000);
        resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
    } else {
        // TODO otherwuse, return error
        resp.set_error_code(cpp2::ErrorCode::E_SYNTAX_ERROR);
        resp.set_error_msg("Syntax error: Unknown statement");
    }

    resp.set_latency_in_ms(dur.elapsedInMSec());

    folly::Promise<cpp2::ExecutionResponse> promise;
    promise.setValue(std::move(resp));
    return promise.getFuture();
}


const char* GraphDbServiceHandler::getErrorStr(cpp2::ErrorCode result) {
    switch (result) {
    case cpp2::ErrorCode::SUCCEEDED:
        return "Succeeded";
    /**********************
     * Server side errors
     **********************/
    case cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD:
        return "Bad username/password";
    case cpp2::ErrorCode::E_SESSION_INVALID:
        return "The session is invalid";
    case cpp2::ErrorCode::E_SESSION_TIMEOUT:
        return "The session timed out";
    case cpp2::ErrorCode::E_SYNTAX_ERROR:
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

