/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/GraphService.h"
#include "time/Duration.h"
#include "graph/RequestContext.h"
#include "graph/SimpleAuthenticator.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    sessionManager_ = std::make_unique<SessionManager>();
    executionEngine_ = std::make_unique<ExecutionEngine>();
    authenticator_ = std::make_unique<SimpleAuthenticator>();

    return executionEngine_->init(std::move(ioExecutor));
}


folly::Future<cpp2::AuthResponse> GraphService::future_authenticate(
        const std::string& username,
        const std::string& password) {
    auto *peer = getConnectionContext()->getPeerAddress();
    FVLOG2("Authenticating user %s from %s", username.c_str(), peer->describe().c_str());

    RequestContext<cpp2::AuthResponse> ctx;
    auto session = sessionManager_->createSession();
    session->setUser(username);
    ctx.setSession(std::move(session));

    if (authenticator_->auth(username, password)) {
        ctx.resp().set_error_code(cpp2::ErrorCode::SUCCEEDED);
        ctx.resp().set_session_id(ctx.session()->id());
    } else {
        sessionManager_->removeSession(ctx.session()->id());
        ctx.resp().set_error_code(cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD);
        ctx.resp().set_error_msg(getErrorStr(cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD));
    }

    ctx.finish();
    return ctx.future();
}


void GraphService::signout(int64_t sessionId) {
    VLOG(2) << "Sign out session " << sessionId;
    sessionManager_->removeSession(sessionId);
}


folly::Future<cpp2::ExecutionResponse>
GraphService::future_execute(int64_t sessionId, const std::string& query) {
    auto ctx = std::make_unique<RequestContext<cpp2::ExecutionResponse>>();
    ctx->setQuery(query);
    ctx->setRunner(getThreadManager());
    auto future = ctx->future();
    {
        auto result = sessionManager_->findSession(sessionId);
        if (!result.ok()) {
            FLOG_ERROR("Session not found, id[%ld]", sessionId);
            ctx->resp().set_error_code(cpp2::ErrorCode::E_SESSION_INVALID);
            ctx->resp().set_error_msg(result.status().toString());
            ctx->finish();
            return future;
        }
        ctx->setSession(std::move(result).value());
    }
    executionEngine_->execute(std::move(ctx));

    return future;
}


const char* GraphService::getErrorStr(cpp2::ErrorCode result) {
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

}  // namespace graph
}  // namespace nebula

