/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/GraphService.h"
#include "graph/RequestContext.h"
#include "storage/client/StorageClient.h"
#include "graph/GraphFlags.h"
#include "common/encryption/MD5Utils.h"

DECLARE_string(meta_server_addrs);
DECLARE_bool(local_config);

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!addrs.ok()) {
        return addrs.status();
    }

    meta::MetaClientOptions options;
    options.serviceName_ = "graph";
    options.skipConfig_ = FLAGS_local_config;
    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                     std::move(addrs.value()),
                                                     options);
    // load data try 3 time
    bool loadDataOk = metaClient_->waitForMetadReady(FLAGS_meta_client_retry_times);
    if (!loadDataOk) {
        // Resort to retrying in the background
        LOG(WARNING) << "Failed to synchronously wait for meta service ready";
    }

    sessionManager_ = std::make_unique<SessionManager>();
    executionEngine_ = std::make_unique<ExecutionEngine>(metaClient_.get());

    return executionEngine_->init(std::move(ioExecutor));
}


folly::Future<cpp2::AuthResponse> GraphService::future_authenticate(
        const std::string& username,
        const std::string& password) {
    auto *peer = getConnectionContext()->getPeerAddress();
    LOG(INFO) << "Authenticating user " << username << " from " << peer->describe();

    RequestContext<cpp2::AuthResponse> ctx;
    auto session = sessionManager_->createSession();
    session->setAccount(username);
    ctx.setSession(std::move(session));

    if (!FLAGS_enable_authorize) {
        onHandle(ctx, cpp2::ErrorCode::SUCCEEDED);
    } else if (auth(username, password)) {
        auto roles = metaClient_->getRolesByUserFromCache(username);
        for (const auto& role : roles) {
            ctx.session()->setRole(role.get_space_id(), toRole(role.get_role_type()));
        }
        onHandle(ctx, cpp2::ErrorCode::SUCCEEDED);
    } else {
        onHandle(ctx, cpp2::ErrorCode::E_BAD_USERNAME_PASSWORD);
    }

    ctx.finish();
    return ctx.future();
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

session::Role GraphService::toRole(nebula::cpp2::RoleType role) {
    switch (role) {
        case nebula::cpp2::RoleType::GOD : {
            return session::Role::GOD;
        }
        case nebula::cpp2::RoleType::ADMIN : {
            return session::Role::ADMIN;
        }
        case nebula::cpp2::RoleType::DBA : {
            return session::Role::DBA;
        }
        case nebula::cpp2::RoleType::USER : {
            return session::Role::USER;
        }
        case nebula::cpp2::RoleType::GUEST : {
            return session::Role::GUEST;
        }
    }
    return session::Role::INVALID_ROLE;
}

bool GraphService::auth(const std::string& username, const std::string& password) {
    std::string auth_type = FLAGS_auth_type;
    folly::toLowerAscii(auth_type);
    if (!auth_type.compare("password")) {
        auto authenticator = std::make_unique<PasswordAuthenticator>(metaClient_.get());
        return authenticator->auth(username, encryption::MD5Utils::md5Encode(password));
    } else if (!auth_type.compare("cloud")) {
        auto authenticator = std::make_unique<CloudAuthenticator>(metaClient_.get());
        return authenticator->auth(username, password);
    }
    return false;
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
    case cpp2::ErrorCode::E_USER_NOT_FOUND:
        return "User not exist";
    case cpp2::ErrorCode::E_BAD_PERMISSION:
        return "Permission denied";
    /**********************
     * Unknown error
     **********************/
    default:
        return "Unknown error";
    }
}

}  // namespace graph
}  // namespace nebula

