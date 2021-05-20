/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "service/SessionManager.h"
#include "service/GraphFlags.h"

namespace nebula {
namespace graph {

SessionManager::SessionManager() {
    scavenger_ = std::make_unique<thread::GenericWorker>();
    auto ok = scavenger_->start("session-manager");
    DCHECK(ok);
    auto bound = std::bind(&SessionManager::reclaimExpiredSessions, this);
    scavenger_->addRepeatTask(FLAGS_session_reclaim_interval_secs * 1000, std::move(bound));
}


SessionManager::~SessionManager() {
    if (scavenger_ != nullptr) {
        scavenger_->stop();
        scavenger_->wait();
        scavenger_.reset();
    }
}


StatusOr<std::shared_ptr<Session>>
SessionManager::findSession(int64_t id) {
    folly::RWSpinLock::ReadHolder holder(rwlock_);
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return Status::Error("Session `%ld' has expired", id);
    }
    return iter->second;
}


std::shared_ptr<Session> SessionManager::createSession() {
    folly::RWSpinLock::WriteHolder holder(rwlock_);
    auto sid = newSessionId();
    while (true) {
        if (activeSessions_.count(sid) == 0UL) {
            break;
        }
        // This ID is in use already, try another one
        sid = newSessionId();
    }
    DCHECK_NE(sid, 0L);
    auto session = Session::create(sid);
    activeSessions_[sid] = session;
    session->charge();
    return session;
}


std::shared_ptr<Session> SessionManager::removeSession(int64_t id) {
    folly::RWSpinLock::WriteHolder holder(rwlock_);
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return nullptr;
    }
    auto session = std::move(iter->second);
    activeSessions_.erase(iter);
    return session;
}


int64_t SessionManager::newSessionId() {
    int64_t id = ++nextId_;
    DCHECK_GT(id, 0);
    return id;
}


// TODO(dutor) Now we do a brute-force scanning, of course we could make it more efficient.
void SessionManager::reclaimExpiredSessions() {
    if (FLAGS_session_idle_timeout_secs == 0) {
        return;
    }
    folly::RWSpinLock::WriteHolder holder(rwlock_);
    if (activeSessions_.empty()) {
        return;
    }

    FVLOG3("Try to reclaim expired sessions out of %lu ones", activeSessions_.size());
    auto iter = activeSessions_.begin();
    auto end = activeSessions_.end();
    while (iter != end) {
        auto *session = iter->second.get();
        int32_t idleSecs = session->idleSeconds();
        if (idleSecs < FLAGS_session_idle_timeout_secs) {
            ++iter;
            continue;
        }
        FLOG_INFO("Session %ld has expired", session->id());
        iter = activeSessions_.erase(iter);
    }
}

}   // namespace graph
}   // namespace nebula
