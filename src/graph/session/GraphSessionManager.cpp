// Copyright (c) 2018 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/session/GraphSessionManager.h"

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/stats/StatsManager.h"
#include "common/time/WallClock.h"
#include "graph/service/GraphFlags.h"
#include "graph/stats/GraphStats.h"

namespace nebula {
namespace graph {

// During construction, GraphSessionManager will start a background thread to periodically
// reclaim expired sessions and update session information to the meta server.
GraphSessionManager::GraphSessionManager(meta::MetaClient* metaClient, const HostAddr& hostAddr)
    : SessionManager<ClientSession>(metaClient, hostAddr) {
  scavenger_->addDelayTask(
      FLAGS_session_reclaim_interval_secs * 1000, &GraphSessionManager::threadFunc, this);
}

folly::Future<StatusOr<std::shared_ptr<ClientSession>>> GraphSessionManager::findSession(
    SessionID id, folly::Executor* runner) {
  auto sessionPtr = findSessionFromCache(id);
  if (sessionPtr != nullptr) {
    return folly::makeFuture(sessionPtr).via(runner);
  }

  return findSessionFromMetad(id, runner);
}

std::shared_ptr<ClientSession> GraphSessionManager::findSessionFromCache(SessionID id) {
  auto iter = activeSessions_.find(id);
  if (iter == activeSessions_.end()) {
    return nullptr;
  }
  VLOG(2) << "Find session from cache: " << id;
  return iter->second;
}

folly::Future<StatusOr<std::shared_ptr<ClientSession>>> GraphSessionManager::findSessionFromMetad(
    SessionID id, folly::Executor* runner) {
  VLOG(1) << "Find session `" << id << "' from metad";
  // local cache not found, need to get from metad
  auto addSession = [this, id](auto&& resp) -> StatusOr<std::shared_ptr<ClientSession>> {
    if (!resp.ok()) {
      LOG(ERROR) << "Get session `" << id << "' failed:" << resp.status();
      return Status::Error("Session `%ld' not found: %s", id, resp.status().toString().c_str());
    }
    auto session = resp.value().get_session();
    session.queries_ref()->clear();
    auto spaceName = session.get_space_name();
    SpaceInfo spaceInfo;
    if (!spaceName.empty()) {
      VLOG(2) << "Get session with spaceName: " << spaceName;
      // get spaceInfo
      auto spaceId = metaClient_->getSpaceIdByNameFromCache(spaceName);
      if (!spaceId.ok()) {
        LOG(ERROR) << "Get session with unknown space: " << spaceName;
        return Status::Error("Get session with unknown space: %s", spaceName.c_str());
      } else {
        auto spaceDesc = metaClient_->getSpaceDesc(spaceId.value());
        if (!spaceDesc.ok()) {
          LOG(ERROR) << "Get session with Unknown space: " << spaceName;
        }
        spaceInfo.id = spaceId.value();
        spaceInfo.name = spaceName;
        spaceInfo.spaceDesc = std::move(spaceDesc).value();
      }
    }

    {
      auto findPtr = activeSessions_.find(id);
      if (findPtr == activeSessions_.end()) {
        VLOG(1) << "Add session id: " << id << " from metad";
        session.graph_addr_ref() = myAddr_;
        auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
        sessionPtr->charge();
        auto ret = activeSessions_.emplace(id, sessionPtr);
        if (!ret.second) {
          return Status::Error("Insert session to local cache failed.");
        }
        std::string key = session.get_user_name() + session.get_client_ip();
        addSessionCount(key);

        // update the space info to sessionPtr
        if (!spaceName.empty()) {
          sessionPtr->setSpace(std::move(spaceInfo));
        }
        updateSessionInfo(sessionPtr.get());
        return sessionPtr;
      }
      updateSessionInfo(findPtr->second.get());
      return findPtr->second;
    }
  };
  return metaClient_->getSession(id).via(runner).thenValue(addSession);
}

std::vector<meta::cpp2::Session> GraphSessionManager::getSessionFromLocalCache() const {
  std::vector<meta::cpp2::Session> sessions;
  sessions.reserve(activeSessions_.size());
  for (auto& it : activeSessions_) {
    sessions.emplace_back(it.second->getSession());
  }
  return sessions;
}

folly::Future<StatusOr<std::shared_ptr<ClientSession>>> GraphSessionManager::createSession(
    const std::string userName, const std::string clientIp, folly::Executor* runner) {
  // check the number of sessions per user per ip
  std::string key = userName + clientIp;
  auto maxSessions = FLAGS_max_sessions_per_ip_per_user;
  auto uiscFindPtr = sessionCnt(key);
  if (maxSessions > 0 && uiscFindPtr && uiscFindPtr->get() > maxSessions - 1) {
    return Status::Error(
        "Create Session failed: Too many sessions created from %s by user %s. "
        "the threshold is %d. You can change it by modifying '%s' in nebula-graphd.conf",
        clientIp.c_str(),
        userName.c_str(),
        maxSessions,
        "max_sessions_per_ip_per_user");
  }
  auto createCB = [this, userName = userName, clientIp = clientIp](
                      auto&& resp) -> StatusOr<std::shared_ptr<ClientSession>> {
    if (!resp.ok()) {
      LOG(ERROR) << "Create session failed:" << resp.status();
      return Status::Error("Create session failed: %s", resp.status().toString().c_str());
    }
    auto session = resp.value().get_session();
    auto sid = session.get_session_id();
    DCHECK_NE(sid, 0L);
    {
      auto findPtr = activeSessions_.find(sid);
      if (findPtr == activeSessions_.end()) {
        VLOG(1) << "Create session id: " << sid << ", for user: " << userName;
        auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
        sessionPtr->charge();
        auto ret = activeSessions_.emplace(sid, sessionPtr);
        if (!ret.second) {
          return Status::Error("Insert session to local cache failed.");
        }
        std::string sessionKey = userName + clientIp;
        addSessionCount(sessionKey);
        updateSessionInfo(sessionPtr.get());
        return sessionPtr;
      }
      updateSessionInfo(findPtr->second.get());
      return findPtr->second;
    }
  };

  return metaClient_->createSession(userName, myAddr_, clientIp).via(runner).thenValue(createCB);
}

void GraphSessionManager::removeSession(SessionID id) {
  removeMultiSessions({id});
}

int32_t GraphSessionManager::removeMultiSessions(const std::vector<SessionID>& ids) {
  // remove sessions from meta server
  auto resp = metaClient_->removeSessions(ids).get();
  if (!resp.ok()) {
    // it will delete by reclaim
    LOG(ERROR) << "Remove sessions failed: " << resp.status();
    return -1;
  }

  auto killedSessions = resp.value().get_removed_session_ids();

  removeSessionFromLocalCache(killedSessions);
  return killedSessions.size();
}

void GraphSessionManager::threadFunc() {
  reclaimExpiredSessions();
  updateSessionsToMeta();
  scavenger_->addDelayTask(
      FLAGS_session_reclaim_interval_secs * 1000, &GraphSessionManager::threadFunc, this);
}

// TODO(dutor) Now we do a brute-force scanning, of course we could make it more
// efficient.
void GraphSessionManager::reclaimExpiredSessions() {
  DCHECK_GT(FLAGS_session_idle_timeout_secs, 0);
  if (FLAGS_session_idle_timeout_secs == 0) {
    LOG(ERROR) << "Program should not reach here, session_idle_timeout_secs should be an integer "
                  "between 1 and 604800";
    return;
  }

  if (activeSessions_.empty()) {
    return;
  }

  FVLOG3("Try to reclaim expired sessions out of %lu ones", activeSessions_.size());
  std::vector<SessionID> expiredSessions;

  // collect expired sessions
  for (const auto& iter : activeSessions_) {
    int32_t idleSecs = iter.second->idleSeconds();
    VLOG(2) << "SessionId: " << iter.first << ", idleSecs: " << idleSecs;
    if (idleSecs < FLAGS_session_idle_timeout_secs) {
      continue;
    }
    // Only reclaim sessions on the same host
    // TODO: maybe we could reclaim sessions on other inactive hosts
    if (iter.second->getGraphAddr() != myAddr_) {
      continue;
    }
    FLOG_INFO("ClientSession %ld has expired", iter.first);

    expiredSessions.emplace_back(iter.first);
    // TODO: Disconnect the connection of the session
  }

  // Remove expired sessions from meta server
  if (expiredSessions.empty()) {
    return;
  }

  auto resp = metaClient_->removeSessions(std::move(expiredSessions)).get();
  if (!resp.ok()) {
    // TODO: Handle cases where the delete client failed
    LOG(ERROR) << "Remove session failed: " << resp.status();
    return;
  }

  auto killedSessions = resp.value().get_removed_session_ids();
  // Remove expired sessions from local cache
  removeSessionFromLocalCache(killedSessions);
}

void GraphSessionManager::updateSessionsToMeta() {
  std::vector<meta::cpp2::Session> sessions;
  {
    if (activeSessions_.empty()) {
      return;
    }

    for (auto& ses : activeSessions_) {
      VLOG(3) << "Add Update session id: " << ses.second->getSession().get_session_id();
      auto sessionCopy = ses.second->getSession();
      for (auto& query : *sessionCopy.queries_ref()) {
        query.second.duration_ref() =
            time::WallClock::fastNowInMicroSec() - query.second.get_start_time();
      }
      sessions.emplace_back(std::move(sessionCopy));
    }
  }

  // There may be expired queries, and the
  // expired queries will be killed here.
  auto handleKilledQueries = [this](auto&& resp) {
    if (!resp.ok()) {
      LOG(ERROR) << "Update sessions failed: " << resp.status();
      return;
    }

    auto& killedQueriesForEachSession = *resp.value().killed_queries_ref();
    for (auto& killedQueries : killedQueriesForEachSession) {
      auto sessionId = killedQueries.first;
      auto session = activeSessions_.find(sessionId);
      if (session == activeSessions_.end()) {
        continue;
      }
      for (auto& desc : killedQueries.second) {
        if (desc.second.get_graph_addr() != session->second->getGraphAddr()) {
          continue;
        }
        auto epId = desc.first;
        session->second->markQueryKilled(epId);
        VLOG(1) << "Kill query, session: " << sessionId << " plan: " << epId;
      }
    }
  };

  // The response from meta contains sessions that are marked as killed, so we need to clean the
  // local cache and update statistics
  auto handleKilledSessions = [this](auto&& resp) {
    if (!resp.ok()) {
      LOG(ERROR) << "Update sessions failed: " << resp.status();
      return;
    }

    auto killSessions = resp.value().get_killed_sessions();
    removeSessionFromLocalCache(killSessions);
  };

  auto result = metaClient_->updateSessions(sessions).get();
  if (!result.ok()) {
    LOG(ERROR) << "Update sessions failed: " << result.status();
    return;
  }
  handleKilledQueries(result);
  handleKilledSessions(result);
}

void GraphSessionManager::updateSessionInfo(ClientSession* session) {
  session->updateGraphAddr(myAddr_);
  auto roles = metaClient_->getRolesByUserFromCache(session->user());
  for (const auto& role : roles) {
    session->setRole(role.get_space_id(), role.get_role_type());
  }
}

Status GraphSessionManager::init() {
  auto listSessionsRet = metaClient_->listSessions().get();
  if (!listSessionsRet.ok()) {
    return Status::Error("Load sessions from meta failed.");
  }
  int64_t loadSessionCount = 0;
  auto& sessions = *listSessionsRet.value().sessions_ref();
  for (auto& session : sessions) {
    if (session.get_graph_addr() != myAddr_) {
      continue;
    }
    auto sessionId = session.get_session_id();
    auto idleSecs = (time::WallClock::fastNowInMicroSec() - session.get_update_time()) / 1000000;
    VLOG(1) << "session_idle_timeout_secs: " << FLAGS_session_idle_timeout_secs
            << " idleSecs: " << idleSecs;
    if (FLAGS_session_idle_timeout_secs > 0 && idleSecs > FLAGS_session_idle_timeout_secs) {
      // remove session if expired
      VLOG(1) << "Remove session: " << sessionId;
      metaClient_->removeSessions({sessionId});
      continue;
    }
    session.queries_ref()->clear();
    std::string key = session.get_user_name() + session.get_client_ip();
    auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
    auto ret = activeSessions_.emplace(sessionId, sessionPtr);
    if (!ret.second) {
      return Status::Error("Insert session to local cache failed.");
    }
    addSessionCount(key);
    updateSessionInfo(sessionPtr.get());
    loadSessionCount++;
  }
  LOG(INFO) << "Total of " << loadSessionCount << " sessions are loaded";
  return Status::OK();
}

void GraphSessionManager::removeSessionFromLocalCache(const std::vector<SessionID>& ids) {
  for (auto& id : ids) {
    // if the session is not in the current graph, ignore it
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
      continue;
    }
    auto sessionPtr = iter->second;
    activeSessions_.erase(iter);

    // All queries on the session need to be marked as killed.
    sessionPtr->markAllQueryKilled();

    // delete session count from cache
    std::string key =
        sessionPtr->getSession().get_user_name() + sessionPtr->getSession().get_client_ip();
    subSessionCount(key);

    // update stats
    stats::StatsManager::decValue(kNumActiveSessions);
  }
}

}  // namespace graph
}  // namespace nebula
