/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/time/Duration.h"
#include "common/encryption/MD5Utils.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "service/GraphService.h"
#include "service/RequestContext.h"
#include "service/SimpleAuthenticator.h"
#include "service/GraphFlags.h"
#include "service/PasswordAuthenticator.h"
#include "service/CloudAuthenticator.h"

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    sessionManager_ = std::make_unique<SessionManager>();
    queryEngine_ = std::make_unique<QueryEngine>();

    return queryEngine_->init(std::move(ioExecutor));
}


folly::Future<cpp2::AuthResponse> GraphService::future_authenticate(
        const std::string& username,
        const std::string& password) {
    auto *peer = getConnectionContext()->getPeerAddress();
    LOG(INFO) << "Authenticating user " << username << " from " <<  peer->describe();

    RequestContext<cpp2::AuthResponse> ctx;
    auto session = sessionManager_->createSession();
    session->setAccount(username);
    ctx.setSession(std::move(session));

    if (!FLAGS_enable_authorize) {
        onHandle(ctx, cpp2::ErrorCode::SUCCEEDED);
    } else if (auth(username, password)) {
        auto roles = queryEngine_->metaClient()->getRolesByUserFromCache(username);
        for (const auto& role : roles) {
            ctx.session()->setRole(role.get_space_id(), role.get_role_type());
        }
        onHandle(ctx, cpp2::ErrorCode::SUCCEEDED);
    } else {
        onHandle(ctx, cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD);
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
            // ctx->resp().set_error_msg(result.status().toString());
            ctx->finish();
            return future;
        }
        ctx->setSession(std::move(result).value());
    }
    queryEngine_->execute(std::move(ctx));

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

void GraphService::onHandle(RequestContext<cpp2::AuthResponse>& ctx, cpp2::ErrorCode code) {
    ctx.resp().set_error_code(code);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        sessionManager_->removeSession(ctx.session()->id());
        ctx.resp().set_error_msg(getErrorStr(code));
    } else {
        ctx.resp().set_session_id(ctx.session()->id());
    }
}

bool GraphService::auth(const std::string& username, const std::string& password) {
    std::string authType = FLAGS_auth_type;
    folly::toLowerAscii(authType);
    if (!authType.compare("password")) {
        auto authenticator = std::make_unique<PasswordAuthenticator>(queryEngine_->metaClient());
        return authenticator->auth(username, encryption::MD5Utils::md5Encode(password));
    } else if (!authType.compare("cloud")) {
        auto authenticator = std::make_unique<CloudAuthenticator>(queryEngine_->metaClient());
        return authenticator->auth(username, password);
    }
    LOG(WARNING) << "Unknown auth type: " << authType;
    return false;
}

}  // namespace graph
}  // namespace nebula
