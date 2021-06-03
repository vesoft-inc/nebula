/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/time/Duration.h"
#include "common/encryption/MD5Utils.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/version/Version.h"
#include "service/GraphService.h"
#include "service/RequestContext.h"
#include "service/GraphFlags.h"
#include "service/PasswordAuthenticator.h"
#include "service/CloudAuthenticator.h"
#include "stats/StatsDef.h"

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
                          const HostAddr &hostAddr) {
    auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!addrs.ok()) {
        return addrs.status();
    }

    meta::MetaClientOptions options;
    options.serviceName_ = "graph";
    options.skipConfig_ = FLAGS_local_config;
    options.role_ = meta::cpp2::HostRole::GRAPH;
    std::string localIP = network::NetworkUtils::getIPv4FromDevice(FLAGS_listen_netdev).value();
    options.localHost_ = hostAddr;
    options.gitInfoSHA_ = gitInfoSha();;

    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                     std::move(addrs.value()),
                                                     options);

    // load data try 3 time
    bool loadDataOk = metaClient_->waitForMetadReady(3);
    if (!loadDataOk) {
        // Resort to retrying in the background
        LOG(WARNING) << "Failed to synchronously wait for meta service ready";
    }

    sessionManager_ = std::make_unique<SessionManager>(metaClient_.get(), hostAddr);
    queryEngine_ = std::make_unique<QueryEngine>();

    myAddr_ = hostAddr;
    return queryEngine_->init(std::move(ioExecutor), metaClient_.get());
}


folly::Future<AuthResponse> GraphService::future_authenticate(
        const std::string& username,
        const std::string& password) {
    auto *peer = getRequestContext()->getPeerAddress();
    auto clientIp = peer->getAddressStr();
    LOG(INFO) << "Authenticating user " << username << " from " <<  peer->describe();

    auto ctx = std::make_unique<RequestContext<AuthResponse>>();
    auto future = ctx->future();
    // check username and password failed
    if (!auth(username, password)) {
        ctx->resp().errorCode = ErrorCode::E_BAD_USERNAME_PASSWORD;
        ctx->resp().errorMsg.reset(new std::string(getErrorStr(ctx->resp().errorCode)));
        ctx->finish();
        return future;
    }

    if (!sessionManager_->isOutOfConnections()) {
        ctx->resp().errorCode = ErrorCode::E_TOO_MANY_CONNECTIONS;
        ctx->resp().errorMsg.reset(new std::string(getErrorStr(ctx->resp().errorCode)));
        ctx->finish();
        return future;
    }

    auto cb = [user = username, cIp = clientIp, ctx = std::move(ctx)]
               (StatusOr<std::shared_ptr<ClientSession>> ret) mutable {
        VLOG(2) << "Create session doFinish";
        if (!ret.ok()) {
            LOG(ERROR) << "Create session for userName: " << user
                       << ", ip: " << cIp << " failed: " << ret.status();
            ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
            ctx->resp().errorMsg.reset(new std::string("Create session failed."));
            return ctx->finish();
        }
        auto sessionPtr = std::move(ret).value();
        if (sessionPtr == nullptr) {
            LOG(ERROR) << "Get session for sessionId is nullptr";
            ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
            ctx->resp().errorMsg.reset(new std::string("Get session for sessionId is nullptr"));
            return ctx->finish();
        }
        ctx->setSession(sessionPtr);
        ctx->resp().sessionId.reset(new int64_t(ctx->session()->id()));
        return ctx->finish();
    };

    sessionManager_->createSession(username, clientIp, getThreadManager())
            .thenValue(std::move(cb));
    return future;
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
    auto cb = [this, sessionId, ctx = std::move(ctx)]
            (StatusOr<std::shared_ptr<ClientSession>> ret) mutable {
        if (!ret.ok()) {
            LOG(ERROR) << "Get session for sessionId: " << sessionId
                        << " failed: " << ret.status();
            ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
            ctx->resp().errorMsg.reset(
                new std::string("SessionId[%ld] does not exist", sessionId));
            return ctx->finish();
        }
        auto sessionPtr = std::move(ret).value();
        if (sessionPtr == nullptr) {
            LOG(ERROR) << "Get session for sessionId: " << sessionId << " is nullptr";
            ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
            ctx->resp().errorMsg.reset(
                new std::string("SessionId[%ld] does not exist", sessionId));
            return ctx->finish();
        }
        ctx->setSession(std::move(sessionPtr));
        queryEngine_->execute(std::move(ctx));
    };
    sessionManager_->findSession(sessionId, getThreadManager()).thenValue(std::move(cb));
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
            return "Too many connections in the cluster";
        case ErrorCode::E_PARTIAL_SUCCEEDED:
            return "Partial results";
    }
    /**********************
     * Unknown error
     **********************/
    return "Unknown error";
}



bool GraphService::auth(const std::string& username, const std::string& password) {
    if (!FLAGS_enable_authorize) {
        return true;
    }
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
