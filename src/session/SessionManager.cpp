/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "session/SessionManager.h"
#include "service/GraphFlags.h"

namespace nebula {
namespace graph {

SessionManager::SessionManager(meta::MetaClient* metaClient, const HostAddr &hostAddr) {
    metaClient_ = metaClient;
    myAddr_ = hostAddr;
    scavenger_ = std::make_unique<thread::GenericWorker>();
    auto ok = scavenger_->start("session-manager");
    DCHECK(ok);
    scavenger_->addDelayTask(FLAGS_session_reclaim_interval_secs * 1000,
                             &SessionManager::threadFunc,
                             this);
}


SessionManager::~SessionManager() {
    if (scavenger_ != nullptr) {
        scavenger_->stop();
        scavenger_->wait();
        scavenger_.reset();
    }
}

folly::Future<StatusOr<std::shared_ptr<ClientSession>>>
SessionManager::findSession(SessionID id, folly::Executor* runner) {
    // When the sessionId is 0, it means the clients to ping the connection is ok
    if (id == 0) {
        return folly::makeFuture(Status::Error("SessionId is invalid")).via(runner);
    }

    auto sessionPtr = findSessionFromCache(id);
    if (sessionPtr != nullptr) {
        return folly::makeFuture(sessionPtr).via(runner);
    }

    return findSessionFromMetad(id, runner);
}

std::shared_ptr<ClientSession> SessionManager::findSessionFromCache(SessionID id) {
    folly::RWSpinLock::ReadHolder rHolder(rwlock_);
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return nullptr;
    }
    VLOG(2) << "Find session from cache: " << id;
    return iter->second;
}


folly::Future<StatusOr<std::shared_ptr<ClientSession>>>
SessionManager::findSessionFromMetad(SessionID id, folly::Executor* runner) {
    VLOG(1) << "Find session `" << id << "' from metad";
    // local cache not found, need to get from metad
    auto addSession = [this, id] (auto &&resp) -> StatusOr<std::shared_ptr<ClientSession>> {
        if (!resp.ok()) {
            LOG(ERROR) << "Get session `" << id << "' failed:" <<  resp.status();
            return Status::Error(
                    "Session `%ld' not found: %s", id, resp.status().toString().c_str());
        }
        auto session = resp.value().get_session();
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
            folly::RWSpinLock::WriteHolder wHolder(rwlock_);
            auto findPtr = activeSessions_.find(id);
            if (findPtr == activeSessions_.end()) {
                VLOG(1) << "Add session id: " << id << " from metad";
                auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
                sessionPtr->charge();
                activeSessions_[id] = sessionPtr;

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
SessionManager::createSession(const std::string userName,
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
            folly::RWSpinLock::WriteHolder wHolder(rwlock_);
            auto findPtr = activeSessions_.find(sid);
            if (findPtr == activeSessions_.end()) {
                VLOG(1) << "Create session id: " << sid << ", for user: " << userName;
                auto sessionPtr = ClientSession::create(std::move(session), metaClient_);
                sessionPtr->charge();
                activeSessions_[sid] = sessionPtr;
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

void SessionManager::removeSession(SessionID id) {
    folly::RWSpinLock::WriteHolder wHolder(rwlock_);
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return;
    }

    auto resp = metaClient_->removeSession(id).get();
    if (!resp.ok()) {
        // it will delete by reclaim
        LOG(ERROR) << "Remove session `" << id << "' failed: " << resp.status();
    }
    activeSessions_.erase(iter);
}

void SessionManager::threadFunc() {
    reclaimExpiredSessions();
    updateSessionsToMeta();
    scavenger_->addDelayTask(FLAGS_session_reclaim_interval_secs * 1000,
                             &SessionManager::threadFunc,
                             this);
}

// TODO(dutor) Now we do a brute-force scanning, of course we could make it more efficient.
void SessionManager::reclaimExpiredSessions() {
    if (FLAGS_session_idle_timeout_secs == 0) {
        return;
    }

    folly::RWSpinLock::WriteHolder wHolder(rwlock_);
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

        auto resp = metaClient_->removeSession(iter->first).get();
        if (!resp.ok()) {
            // TODO: Handle cases where the delete client failed
            LOG(ERROR) << "Remove session `" << iter->first << "' failed: " << resp.status();
        }
        iter = activeSessions_.erase(iter);
        // TODO: Disconnect the connection of the session
    }
}

void SessionManager::updateSessionsToMeta() {
    std::vector<meta::cpp2::Session> sessions;
    {
        folly::RWSpinLock::ReadHolder rHolder(rwlock_);
        if (activeSessions_.empty()) {
            return;
        }

        for (auto &ses : activeSessions_) {
            if (ses.second->getSession().get_graph_addr() == myAddr_) {
                VLOG(3) << "Add Update session id: " << ses.second->getSession().get_session_id();
                sessions.emplace_back(ses.second->getSession());
            }
        }
    }
    auto resp = metaClient_->updateSessions(sessions).get();
    if (!resp.ok()) {
        LOG(ERROR) << "Update sessions failed: " << resp.status();
    }
}

void SessionManager::updateSessionInfo(ClientSession* session) {
    session->updateGraphAddr(myAddr_);
    auto roles = metaClient_->getRolesByUserFromCache(session->user());
    for (const auto &role : roles) {
        session->setRole(role.get_space_id(), role.get_role_type());
    }
}
}   // namespace graph
}   // namespace nebula
