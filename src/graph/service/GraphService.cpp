/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/GraphService.h"

#include <proxygen/lib/utils/CryptUtil.h>

#include <boost/filesystem.hpp>

#include "clients/storage/StorageClient.h"
#include "common/base/Base.h"
#include "common/stats/StatsManager.h"
#include "common/time/Duration.h"
#include "common/time/TimezoneInfo.h"
#include "graph/service/CloudAuthenticator.h"
#include "graph/service/GraphFlags.h"
#include "graph/service/PasswordAuthenticator.h"
#include "graph/service/RequestContext.h"
#include "graph/stats/GraphStats.h"
#include "version/Version.h"

namespace nebula {
namespace graph {

// The default value is 28800 seconds
const int64_t clientAddrTimeout = FLAGS_client_idle_timeout_secs;

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
  options.rootPath_ = boost::filesystem::current_path().string();

  metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor, std::move(addrs.value()), options);

  // Load data try 3 time
  bool loadDataOk = metaClient_->waitForMetadReady(3);
  if (!loadDataOk) {
    // Resort to retrying in the background
    LOG(ERROR) << "Failed to wait for meta service ready synchronously.";
    return Status::Error("Failed to wait for meta service ready synchronously.");
  }

  sessionManager_ = std::make_unique<GraphSessionManager>(metaClient_.get(), hostAddr);
  auto initSessionMgrStatus = sessionManager_->init();
  if (!initSessionMgrStatus.ok()) {
    LOG(ERROR) << "Failed to initialize session manager: " << initSessionMgrStatus.toString();
    return Status::Error("Failed to initialize session manager: %s",
                         initSessionMgrStatus.toString().c_str());
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
  // Check username and password failed
  // Check whether the client has called verifyClientVersion()
  auto clientAddr = HostAddr(peer->getAddressStr(), peer->getPort());
  auto authResult = auth(username, password, clientAddr);
  if (!authResult.ok()) {
    ctx->resp().errorCode = ErrorCode::E_BAD_USERNAME_PASSWORD;
    ctx->resp().errorMsg.reset(new std::string(authResult.toString()));
    ctx->finish();
    stats::StatsManager::addValue(kNumAuthFailedSessions);
    stats::StatsManager::addValue(kNumAuthFailedSessionsBadUserNamePassword);
    return future;
  }

  if (sessionManager_->isOutOfConnections()) {
    ctx->resp().errorCode = ErrorCode::E_TOO_MANY_CONNECTIONS;
    ctx->resp().errorMsg.reset(new std::string("Too many connections in the cluster"));
    ctx->finish();
    stats::StatsManager::addValue(kNumAuthFailedSessions);
    stats::StatsManager::addValue(kNumAuthFailedSessionsOutOfMaxAllowed);
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
    stats::StatsManager::addValue(kNumOpenedSessions);
    stats::StatsManager::addValue(kNumActiveSessions);
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
  stats::StatsManager::decValue(kNumActiveSessions);
}

folly::Future<ExecutionResponse> GraphService::future_executeWithParameter(
    int64_t sessionId,
    const std::string& query,
    const std::unordered_map<std::string, Value>& parameterMap) {
  auto ctx = std::make_unique<RequestContext<ExecutionResponse>>();
  ctx->setQuery(query);
  ctx->setRunner(getThreadManager());
  ctx->setSessionMgr(sessionManager_.get());
  auto future = ctx->future();
  // When the sessionId is 0, it means the clients to ping the connection is ok
  if (sessionId == 0) {
    ctx->resp().errorCode = ErrorCode::E_SESSION_INVALID;
    ctx->resp().errorMsg = std::make_unique<std::string>("Invalid session id");
    ctx->finish();
    return future;
  }
  auto cb = [this, sessionId, ctx = std::move(ctx), parameterMap = std::move(parameterMap)](
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
    stats::StatsManager::addValue(kNumQueries);
    stats::StatsManager::addValue(kNumActiveQueries);
    if (FLAGS_enable_space_level_metrics && sessionPtr->space().name != "") {
      stats::StatsManager::addValue(stats::StatsManager::counterWithLabels(
          kNumQueries, {{"space", sessionPtr->space().name}}));
      stats::StatsManager::addValue(stats::StatsManager::counterWithLabels(
          kNumActiveQueries, {{"space", sessionPtr->space().name}}));
    }
    auto& spaceName = sessionPtr->space().name;
    ctx->setSession(std::move(sessionPtr));
    ctx->setParameterMap(parameterMap);
    queryEngine_->execute(std::move(ctx));
    stats::StatsManager::decValue(kNumActiveQueries);
    if (FLAGS_enable_space_level_metrics && spaceName != "") {
      stats::StatsManager::decValue(
          stats::StatsManager::counterWithLabels(kNumActiveQueries, {{"space", spaceName}}));
    }
  };
  sessionManager_->findSession(sessionId, getThreadManager()).thenValue(std::move(cb));
  return future;
}

folly::Future<ExecutionResponse> GraphService::future_execute(int64_t sessionId,
                                                              const std::string& query) {
  return future_executeWithParameter(sessionId, query, std::unordered_map<std::string, Value>{});
}

folly::Future<std::string> GraphService::future_executeJson(int64_t sessionId,
                                                            const std::string& query) {
  return future_executeJsonWithParameter(
      sessionId, query, std::unordered_map<std::string, Value>{});
}

folly::Future<std::string> GraphService::future_executeJsonWithParameter(
    int64_t sessionId,
    const std::string& query,
    const std::unordered_map<std::string, Value>& parameterMap) {
  return future_executeWithParameter(sessionId, query, parameterMap).thenValue([](auto&& resp) {
    return folly::toJson(resp.toJson());
  });
}

Status GraphService::auth(const std::string& username,
                          const std::string& password,
                          const HostAddr& clientIp) {
  auto metaClient = queryEngine_->metaClient();

  // TODO(Aiee) This is a walkaround to address the problem that using a lower version(< v2.6.0)
  // client to connect with higher version(>= v3.0.0) Nebula service will cause a crash.
  //
  // Only the clients since v2.6.0 will call verifyVersion(), thus we could determine whether the
  // client version is lower than v2.6.0
  auto clientAddrIt = metaClient->getClientAddrMap().find(clientIp);
  if (clientAddrIt == metaClient->getClientAddrMap().end()) {
    return Status::Error(
        folly::sformat("The version of the client sending request from {} is lower than v2.6.0, "
                       "please update the client.",
                       clientIp.toString()));
  }

  // Skip authentication if FLAGS_enable_authorize is false
  if (!FLAGS_enable_authorize) {
    return Status::OK();
  }

  // Authenticate via diffrent auth type
  if (FLAGS_auth_type == "password") {
    // Auth with PasswordAuthenticator
    auto authenticator = std::make_unique<PasswordAuthenticator>(metaClient);
    return authenticator->auth(username, proxygen::md5Encode(folly::StringPiece(password)));
  } else if (FLAGS_auth_type == "cloud") {
    // Cloud user and native user will be mixed.
    // Since cloud user and native user has the same transport protocol,
    // There is no way to identify which one is in the graph layer，
    // let's check the native user's password first, then cloud user.
    auto pwdAuth = std::make_unique<PasswordAuthenticator>(metaClient);
    return pwdAuth->auth(username, proxygen::md5Encode(folly::StringPiece(password)));
    auto cloudAuth = std::make_unique<CloudAuthenticator>(metaClient);
    return cloudAuth->auth(username, password);
  }
  LOG(WARNING) << "Unknown auth type: " << FLAGS_auth_type;
  return Status::Error("Unknown auth type: %s", FLAGS_auth_type.c_str());
}

folly::Future<cpp2::VerifyClientVersionResp> GraphService::future_verifyClientVersion(
    const cpp2::VerifyClientVersionReq& req) {
  std::unordered_set<std::string> whiteList;
  folly::splitTo<std::string>(
      ":", FLAGS_client_white_list, std::inserter(whiteList, whiteList.begin()));
  cpp2::VerifyClientVersionResp resp;

  if (FLAGS_enable_client_white_list && whiteList.find(req.get_version()) == whiteList.end()) {
    resp.error_code_ref() = nebula::cpp2::ErrorCode::E_CLIENT_SERVER_INCOMPATIBLE;
    resp.error_msg_ref() = folly::stringPrintf(
        "Graph client version(%s) is not accepted, current graph client white list: %s.",
        req.get_version().c_str(),
        FLAGS_client_white_list.c_str());
  } else {
    resp.error_code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  // The client sent request has a version >= v2.6.0, mark the address as valid
  auto* peer = getRequestContext()->getPeerAddress();
  auto clientAddr = HostAddr(peer->getAddressStr(), peer->getPort());

  auto ttlTimestamp = time::WallClock::fastNowInSec() + clientAddrTimeout;
  auto clientAddrMap = &metaClient_->getClientAddrMap();
  clientAddrMap->insert_or_assign(clientAddr, ttlTimestamp);
  return folly::makeFuture<cpp2::VerifyClientVersionResp>(std::move(resp));
}
}  // namespace graph
}  // namespace nebula
