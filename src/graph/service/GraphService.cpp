/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/service/GraphService.h"

#include "clients/storage/GraphStorageClient.h"
#include "common/base/Base.h"
#include "common/encryption/MD5Utils.h"
#include "common/time/Duration.h"
#include "common/time/TimezoneInfo.h"
#include "graph/service/CloudAuthenticator.h"
#include "graph/service/GraphFlags.h"
#include "graph/service/PasswordAuthenticator.h"
#include "graph/service/RequestContext.h"
#include "graph/stats/StatsDef.h"
#include "version/Version.h"

namespace nebula {
namespace graph {

Status GraphService::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
                          const HostAddr& hostAddr) {
  auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
  if (!addrs.ok()) {
    return addrs.status();
  }

  meta::MetaClientOptions options;
  options.serviceName_ = "graph";
  options.skipConfig_ = FLAGS_local_config;
  options.role_ = meta::cpp2::HostRole::GRAPH;
  options.localHost_ = hostAddr;
  options.gitInfoSHA_ = gitInfoSha();

  metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor, std::move(addrs.value()), options);

  // load data try 3 time
  bool loadDataOk = metaClient_->waitForMetadReady(3);
  if (!loadDataOk) {
    // Resort to retrying in the background
    LOG(WARNING) << "Failed to wait for meta service ready synchronously.";
  }

  sessionManager_ = std::make_unique<GraphSessionManager>(metaClient_.get(), hostAddr);
  auto initSessionMgrStatus = sessionManager_->init();
  if (!initSessionMgrStatus.ok()) {
    LOG(WARNING) << "Failed to initialize session manager: " << initSessionMgrStatus.toString();
  }

  queryEngine_ = std::make_unique<QueryEngine>();
  return queryEngine_->init(std::move(ioExecutor), metaClient_.get());
}

folly::Future<AuthResponse> GraphService::future_authenticate(const std::string& username,
                                                              const std::string& password) {
  auto* peer = getRequestContext()->getPeerAddress();
  auto clientIp = peer->getAddressStr();
  LOG(INFO) << "Authenticating user " << username << " from " << peer->describe();

  auto ctx = std::make_unique<RequestContext<AuthResponse>>();
  auto future = ctx->future();
  // check username and password failed
  if (!auth(username, password)) {
    ctx->resp().errorCode = ErrorCode::E_BAD_USERNAME_PASSWORD;
    ctx->resp().errorMsg.reset(new std::string("Bad username/password"));
    ctx->finish();
    return future;
  }

  if (!sessionManager_->isOutOfConnections()) {
    ctx->resp().errorCode = ErrorCode::E_TOO_MANY_CONNECTIONS;
    ctx->resp().errorMsg.reset(new std::string("Too many connections in the cluster"));
    ctx->finish();
    return future;
  }

  auto cb = [user = username, cIp = clientIp, ctx = std::move(ctx)](
                StatusOr<std::shared_ptr<ClientSession>> ret) mutable {
    VLOG(2) << "Create session doFinish";
    if (!ret.ok()) {
      LOG(ERROR) << "Create session for userName: " << user << ", ip: " << cIp
                 << " failed: " << ret.status();
      ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
      ctx->resp().errorMsg.reset(new std::string(ret.status().toString()));
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
    ctx->resp().timeZoneOffsetSeconds.reset(
        new int32_t(time::Timezone::getGlobalTimezone().utcOffsetSecs()));
    ctx->resp().timeZoneName.reset(
        new std::string(time::Timezone::getGlobalTimezone().stdZoneName()));
    return ctx->finish();
  };

  sessionManager_->createSession(username, clientIp, getThreadManager()).thenValue(std::move(cb));
  return future;
}

void GraphService::signout(int64_t sessionId) {
  VLOG(2) << "Sign out session " << sessionId;
  sessionManager_->removeSession(sessionId);
}

folly::Future<ExecutionResponse> GraphService::future_execute(int64_t sessionId,
                                                              const std::string& query) {
  auto ctx = std::make_unique<RequestContext<ExecutionResponse>>();
  ctx->setQuery(query);
  ctx->setRunner(getThreadManager());
  ctx->setSessionMgr(sessionManager_.get());
  auto future = ctx->future();
  stats::StatsManager::addValue(kNumQueries);
  // When the sessionId is 0, it means the clients to ping the connection is ok
  if (sessionId == 0) {
    ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
    ctx->resp().errorMsg = std::make_unique<std::string>("Invalid session id");
    ctx->finish();
    return future;
  }
  auto cb = [this, sessionId, ctx = std::move(ctx)](
                StatusOr<std::shared_ptr<ClientSession>> ret) mutable {
    if (!ret.ok()) {
      LOG(ERROR) << "Get session for sessionId: " << sessionId << " failed: " << ret.status();
      ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
      ctx->resp().errorMsg.reset(new std::string(folly::stringPrintf(
          "Get sessionId[%ld] failed: %s", sessionId, ret.status().toString().c_str())));
      return ctx->finish();
    }
    auto sessionPtr = std::move(ret).value();
    if (sessionPtr == nullptr) {
      LOG(ERROR) << "Get session for sessionId: " << sessionId << " is nullptr";
      ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
      ctx->resp().errorMsg.reset(
          new std::string(folly::stringPrintf("SessionId[%ld] does not exist", sessionId)));
      return ctx->finish();
    }
    ctx->setSession(std::move(sessionPtr));
    queryEngine_->execute(std::move(ctx));
  };
  sessionManager_->findSession(sessionId, getThreadManager()).thenValue(std::move(cb));
  return future;
}

folly::Future<std::string> GraphService::future_executeJson(int64_t sessionId,
                                                            const std::string& query) {
  return future_execute(sessionId, query).thenValue([](ExecutionResponse&& resp) {
    return folly::toJson(resp.toJson());
  });
}

bool GraphService::auth(const std::string& username, const std::string& password) {
  if (!FLAGS_enable_authorize) {
    return true;
  }

  if (FLAGS_auth_type == "password") {
    auto authenticator = std::make_unique<PasswordAuthenticator>(queryEngine_->metaClient());
    return authenticator->auth(username, encryption::MD5Utils::md5Encode(password));
  } else if (FLAGS_auth_type == "cloud") {
    // Cloud user and native user will be mixed.
    // Since cloud user and native user has the same transport protocol,
    // There is no way to identify which one is in the graph layer，
    // let's check the native user's password first, then cloud user.
    auto pwdAuth = std::make_unique<PasswordAuthenticator>(queryEngine_->metaClient());
    if (pwdAuth->auth(username, encryption::MD5Utils::md5Encode(password))) {
      return true;
    }
    auto cloudAuth = std::make_unique<CloudAuthenticator>(queryEngine_->metaClient());
    return cloudAuth->auth(username, password);
  }
  LOG(WARNING) << "Unknown auth type: " << FLAGS_auth_type;
  return false;
}

folly::Future<cpp2::VerifyClientVersionResp> GraphService::future_verifyClientVersion(
    const cpp2::VerifyClientVersionReq& req) {
  std::unordered_set<std::string> whiteList;
  folly::splitTo<std::string>(
      ":", FLAGS_client_white_list, std::inserter(whiteList, whiteList.begin()));
  cpp2::VerifyClientVersionResp resp;
  if (FLAGS_enable_client_white_list && whiteList.find(req.get_version()) == whiteList.end()) {
    resp.set_error_code(nebula::cpp2::ErrorCode::E_CLIENT_SERVER_INCOMPATIBLE);
    resp.set_error_msg(folly::stringPrintf(
        "Graph client version(%s) is not accepted, current graph client white list: %s.",
        req.get_version().c_str(),
        FLAGS_client_white_list.c_str()));
  } else {
    resp.set_error_code(nebula::cpp2::ErrorCode::SUCCEEDED);
  }
  return folly::makeFuture<cpp2::VerifyClientVersionResp>(std::move(resp));
}
}  // namespace graph
}  // namespace nebula
