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
#include "service/GraphFlags.h"
#include "service/PasswordAuthenticator.h"
#include "service/CloudAuthenticator.h"
#include "stats/StatsDef.h"

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    sessionManager_ = std::make_unique<SessionManager>();
    queryEngine_ = std::make_unique<QueryEngine>();

    return queryEngine_->init(std::move(ioExecutor));
}


folly::Future<AuthResponse> GraphService::future_authenticate(
        const std::string& username,
        const std::string& password) {
    auto *peer = getRequestContext()->getPeerAddress();
    LOG(INFO) << "Authenticating user " << username << " from " <<  peer->describe();

    RequestContext<AuthResponse> ctx;
    auto session = sessionManager_->createSession();
    session->setAccount(username);
    ctx.setSession(std::move(session));

    if (!FLAGS_enable_authorize) {
        onHandle(ctx, ErrorCode::SUCCEEDED);
    } else if (auth(username, password)) {
        auto roles = queryEngine_->metaClient()->getRolesByUserFromCache(username);
        for (const auto& role : roles) {
            ctx.session()->setRole(role.get_space_id(), role.get_role_type());
        }
        onHandle(ctx, ErrorCode::SUCCEEDED);
    } else {
        onHandle(ctx, ErrorCode::E_BAD_USERNAME_PASSWORD);
    }

    ctx.finish();
    return ctx.future();
}


void GraphService::signout(int64_t sessionId) {
    VLOG(2) << "Sign out session " << sessionId;
    sessionManager_->removeSession(sessionId);
}


folly::Future<ExecutionResponse>
GraphService::future_execute(int64_t sessionId, const std::string& query) {
    auto ctx = std::make_unique<RequestContext<ExecutionResponse>>();
    ctx->setQuery(query);
    ctx->setRunner(getThreadManager());
    auto future = ctx->future();
    stats::StatsManager::addValue(kNumQueries);

    {
        // When the sessionId is 0, it means the clients to ping the connection is ok
        if (sessionId == 0) {
            ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
            ctx->resp().errorMsg = std::make_unique<std::string>("Invalid session id");
            ctx->finish();
            return future;
        }
        auto result = sessionManager_->findSession(sessionId);
        if (!result.ok()) {
            FLOG_ERROR("Session not found, id[%ld]", sessionId);
            ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
            ctx->resp().errorMsg = std::make_unique<std::string>(result.status().toString());
            ctx->finish();
            return future;
        }
        ctx->setSession(std::move(result).value());
    }
    queryEngine_->execute(std::move(ctx));

    return future;
}


const char* GraphService::getErrorStr(ErrorCode result) {
    switch (result) {
        case ErrorCode::SUCCEEDED:
            return "Succeeded";
        /**********************
         * Server side errors
         **********************/
        case ErrorCode::E_BAD_USERNAME_PASSWORD:
            return "Bad username/password";
        case ErrorCode::E_SESSION_INVALID:
            return "The session is invalid";
        case ErrorCode::E_SESSION_TIMEOUT:
            return "The session timed out";
        case ErrorCode::E_SYNTAX_ERROR:
            return "Syntax error";
        case ErrorCode::E_SEMANTIC_ERROR:
            return "Semantic error";
        case ErrorCode::E_STATEMENT_EMPTY:
            return "Statement empty";
        case ErrorCode::E_EXECUTION_ERROR:
            return "Execution error";
        case ErrorCode::E_RPC_FAILURE:
            return "RPC failure";
        case ErrorCode::E_DISCONNECTED:
            return "Disconnected";
        case ErrorCode::E_FAIL_TO_CONNECT:
            return "Fail to connect";
        case ErrorCode::E_BAD_PERMISSION:
            return "Bad permission";
        case ErrorCode::E_USER_NOT_FOUND:
            return "User not found";
        case ErrorCode::E_TOO_MANY_CONNECTIONS:
            return "Too many connections";
        case ErrorCode::E_PARTIAL_SUCCEEDED:
            return "Partial results";
    }
    /**********************
     * Unknown error
     **********************/
    return "Unknown error";
}

void GraphService::onHandle(RequestContext<AuthResponse>& ctx, ErrorCode code) {
    ctx.resp().errorCode = code;
    if (code != ErrorCode::SUCCEEDED) {
        sessionManager_->removeSession(ctx.session()->id());
        ctx.resp().errorMsg.reset(new std::string(getErrorStr(code)));
    } else {
        ctx.resp().sessionId.reset(new int64_t(ctx.session()->id()));
    }
}

bool GraphService::auth(const std::string& username, const std::string& password) {
    if (FLAGS_auth_type == "password") {
        auto authenticator = std::make_unique<PasswordAuthenticator>(queryEngine_->metaClient());
        return authenticator->auth(username, encryption::MD5Utils::md5Encode(password));
    } else if (FLAGS_auth_type == "cloud") {
        auto authenticator = std::make_unique<CloudAuthenticator>(queryEngine_->metaClient());
        return authenticator->auth(username, password);
    }
    LOG(WARNING) << "Unknown auth type: " << FLAGS_auth_type;
    return false;
}

}  // namespace graph
}  // namespace nebula
