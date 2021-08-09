/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "session/GraphSessionManager.h"
#include "common/base/Base.h"
#include "common/time/WallClock.h"
#include "service/GraphFlags.h"

namespace nebula {
namespace graph {

GraphSessionManager::GraphSessionManager(meta::MetaClient* metaClient, const HostAddr& hostAddr)
    : SessionManager<ClientSession>(metaClient, hostAddr) {
    scavenger_->addDelayTask(
        FLAGS_session_reclaim_interval_secs * 1000, &GraphSessionManager::threadFunc, this);
}

folly::Future<StatusOr<std::shared_ptr<ClientSession>>>
GraphSessionManager::findSession(SessionID id, folly::Executor* runner) {
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


folly::Future<StatusOr<std::shared_ptr<ClientSession>>>
GraphSessionManager::findSessionFromMetad(SessionID id, folly::Executor* runner) {
    VLOG(1) << "Find session `" << id << "' from metad";
    // local cache not found, need to get from metad
    auto addSession = [this, id] (auto &&resp) -> StatusOr<std::shared_ptr<ClientSession>> {
        if (!resp.ok()) {
            LOG(ERROR) << "Get session `" << id << "' failed:" <<  resp.status();
            return Status::Error(
                    "Session `%ld' not found: %s", id, resp.status().toString().c_str());
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
                session.set_graph_addr(myAddr_);
                auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
                sessionPtr->charge();
                auto ret = activeSessions_.emplace(id, sessionPtr);
                if (!ret.second) {
                    return Status::Error("Insert session to local cache failed.");
                }

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
    return metaClient_->getSession(id)
            .via(runner)
            .thenValue(addSession);
}

folly::Future<StatusOr<std::shared_ptr<ClientSession>>>
GraphSessionManager::createSession(const std::string userName,
                              const std::string clientIp,
                              folly::Executor* runner) {
    auto createCB = [this, userName = userName]
            (auto && resp) -> StatusOr<std::shared_ptr<ClientSession>> {
        if (!resp.ok()) {
            LOG(ERROR) << "Create session failed:" <<  resp.status();
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
                updateSessionInfo(sessionPtr.get());
                return sessionPtr;
            }
            updateSessionInfo(findPtr->second.get());
            return findPtr->second;
        }
    };

    return metaClient_->createSession(userName, myAddr_, clientIp)
            .via(runner)
            .thenValue(createCB);
}

void GraphSessionManager::removeSession(SessionID id) {
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return;
    }

    iter->second->markAllQueryKilled();
    auto resp = metaClient_->removeSession(id).get();
    if (!resp.ok()) {
        // it will delete by reclaim
        LOG(ERROR) << "Remove session `" << id << "' failed: " << resp.status();
    }
    activeSessions_.erase(iter);
}

void GraphSessionManager::threadFunc() {
    reclaimExpiredSessions();
    updateSessionsToMeta();
    scavenger_->addDelayTask(FLAGS_session_reclaim_interval_secs * 1000,
                             &GraphSessionManager::threadFunc,
                             this);
}

// TODO(dutor) Now we do a brute-force scanning, of course we could make it more efficient.
void GraphSessionManager::reclaimExpiredSessions() {
    if (FLAGS_session_idle_timeout_secs == 0) {
        return;
    }

    if (activeSessions_.empty()) {
        return;
    }

    FVLOG3("Try to reclaim expired sessions out of %lu ones", activeSessions_.size());
    auto iter = activeSessions_.begin();
    auto end = activeSessions_.end();
    while (iter != end) {
        int32_t idleSecs = iter->second->idleSeconds();
        VLOG(2) << "SessionId: " << iter->first << ", idleSecs: " << idleSecs;
        if (idleSecs < FLAGS_session_idle_timeout_secs) {
            ++iter;
            continue;
        }
        FLOG_INFO("ClientSession %ld has expired", iter->first);

        iter->second->markAllQueryKilled();
        auto resp = metaClient_->removeSession(iter->first).get();
        if (!resp.ok()) {
            // TODO: Handle cases where the delete client failed
            LOG(ERROR) << "Remove session `" << iter->first << "' failed: " << resp.status();
        }
        iter = activeSessions_.erase(iter);
        // TODO: Disconnect the connection of the session
    }
}

void GraphSessionManager::updateSessionsToMeta() {
    std::vector<meta::cpp2::Session> sessions;
    {
        if (activeSessions_.empty()) {
            return;
        }

        for (auto &ses : activeSessions_) {
            VLOG(3) << "Add Update session id: " << ses.second->getSession().get_session_id();
            auto sessionCopy = ses.second->getSession();
            for (auto& query : *sessionCopy.queries_ref()) {
                query.second.set_duration(time::WallClock::fastNowInMicroSec() -
                                            query.second.get_start_time());
            }
            sessions.emplace_back(std::move(sessionCopy));
        }
    }

    auto handleKilledQueries = [this] (auto&& resp) {
        if (!resp.ok()) {
            LOG(ERROR) << "Update sessions failed: " << resp.status();
            return Status::Error("Update sessions failed: %s",
                                resp.status().toString().c_str());
        }
        auto& killedQueriesForEachSession = *resp.value().killed_queries_ref();
        for (auto& killedQueries : killedQueriesForEachSession) {
            auto sessionId = killedQueries.first;
            auto session = activeSessions_.find(sessionId);
            if (session == activeSessions_.end()) {
                continue;
            }
            for (auto& desc : killedQueries.second) {
                if (desc.second.get_graph_addr() !=
                    session->second->getGraphAddr()) {
                    continue;
                }
                auto epId = desc.first;
                session->second->markQueryKilled(epId);
                VLOG(1)
                    << "Kill query, session: " << sessionId << " plan: " << epId;
            }
        }
        return Status::OK();
    };

    auto result = metaClient_->updateSessions(sessions)
                      .thenValue(handleKilledQueries)
                      .get();
    if (!result.ok()) {
        LOG(ERROR) << "Update sessions failed: " << result;
    }
}

void GraphSessionManager::updateSessionInfo(ClientSession* session) {
    session->updateGraphAddr(myAddr_);
    auto roles = metaClient_->getRolesByUserFromCache(session->user());
    for (const auto &role : roles) {
        session->setRole(role.get_space_id(), role.get_role_type());
    }
}

Status GraphSessionManager::init() {
    auto listSessionsRet = metaClient_->listSessions().get();
    if (!listSessionsRet.ok()) {
        return Status::Error("Load sessions from meta failed.");
    }
    auto& sessions = *listSessionsRet.value().sessions_ref();
    for (auto& session : sessions) {
        if (session.get_graph_addr() != myAddr_) {
            continue;
        }
        auto sessionId = session.get_session_id();
        auto idleSecs =
            (time::WallClock::fastNowInMicroSec() - session.get_update_time()) / 1000000;
        VLOG(1) << "session_idle_timeout_secs: " << FLAGS_session_idle_timeout_secs
            << " idleSecs: " << idleSecs;
        if (FLAGS_session_idle_timeout_secs > 0 && idleSecs > FLAGS_session_idle_timeout_secs) {
            // remove session if expired
            VLOG(1) << "Remove session: " << sessionId;
            metaClient_->removeSession(sessionId);
            continue;
        }
        session.queries_ref()->clear();
        auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
        auto ret = activeSessions_.emplace(sessionId, sessionPtr);
        if (!ret.second) {
            return Status::Error("Insert session to local cache failed.");
        }
        updateSessionInfo(sessionPtr.get());
    }
    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
