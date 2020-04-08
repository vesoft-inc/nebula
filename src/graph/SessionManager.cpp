/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/SessionManager.h"
#include "graph/GraphFlags.h"
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace nebula {
namespace graph {

SessionManager::SessionManager(meta::MetaClient* client) {
    metaClient_ = client;
    executor_.reset(new folly::CPUThreadPoolExecutor(2));
    runner_.reset(new folly::CPUThreadPoolExecutor(2));
    graphServerAddr_ = FLAGS_graph_server_ip + ":" + folly::to<std::string>(FLAGS_port);
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


StatusOr<std::shared_ptr<session::Session>>
SessionManager::findSession(int64_t id) {
    folly::RWSpinLock::ReadHolder holder(rwlock_);
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return Status::Error("Session `%ld' has expired", id);
    }
    return iter->second.first;
}


std::shared_ptr<session::Session> SessionManager::createSession() {
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
    auto session = session::Session::create(sid);
    auto now = time::WallClock::fastNowInSec();
    activeSessions_[sid] = std::make_pair(session, now);
    activeSessForMeta_[sid] = now;
    session->charge();

    // sessionId, FLAGS_graph_server_ip:FLAGS_port, start_time
    executor_->add([sid, now, this] () {
        this->doAddSession(sid, now);
    });

    return session;
}

void SessionManager::doAddSession(int64_t sid, int64_t startTime) {
    std::vector<nebula::meta::cpp2::SessionItem> sessionItem;
    nebula::meta::cpp2::SessionItem item;
    item.set_session_id(sid);
    item.set_addr(graphServerAddr_);
    item.set_start_time(startTime);
    sessionItem.emplace_back(std::move(item));

    auto future = metaClient_->addSession(std::move(sessionItem));
    auto cb = [sid, this] (auto &&resp) {
        if (!resp.ok()) {
            auto msg = folly::stringPrintf("Add global session %ld on %s failed.",
                                           sid, graphServerAddr_.c_str());
            LOG(ERROR) << msg;
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            auto msg = folly::stringPrintf("Add global session %ld on %s failed.",
                                           sid, graphServerAddr_.c_str());
            LOG(ERROR) << msg;
        }
        return;
    };

    auto error = [sid, this] (auto &&e) {
        auto msg = folly::stringPrintf("Add global session %ld on %s exception: %s.",
                                       sid, graphServerAddr_.c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        return;
    };

    std::move(future).thenValue(cb).thenError(error);
}

void SessionManager::doRemoveSession(std::unordered_map<int64_t, int64_t> removeSess) {
    std::vector<nebula::meta::cpp2::SessionItem> sessionItem;
    for (auto& e : removeSess) {
        nebula::meta::cpp2::SessionItem item;
        item.set_session_id(e.first);
        item.set_addr(graphServerAddr_);
        item.set_start_time(e.second);
        sessionItem.emplace_back(std::move(item));
    }

    auto future = metaClient_->removeSession(std::move(sessionItem));
    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            auto msg = folly::stringPrintf("Romove global session on %s failed.",
                                           graphServerAddr_.c_str());
            LOG(ERROR) << msg;
            return;
        }
        auto ret = std::move(resp).value();
        if (!ret) {
            auto msg = folly::stringPrintf("Romove global session on %s failed.",
                                           graphServerAddr_.c_str());
            LOG(ERROR) << msg;
        }
        return;
    };

    auto error = [this] (auto &&e) {
        auto msg = folly::stringPrintf("Remove global session on %s exception: %s.",
                                       graphServerAddr_.c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        return;
    };

    std::move(future).thenValue(cb).thenError(error);
}

std::shared_ptr<session::Session> SessionManager::removeSession(int64_t id) {
    folly::RWSpinLock::WriteHolder holder(rwlock_);
    auto iter = activeSessions_.find(id);
    if (iter == activeSessions_.end()) {
        return nullptr;
    }
    auto session = std::move(iter->second.first);
    auto startTime = iter->second.second;

    activeSessions_.erase(iter);
    activeSessForMeta_.erase(id);

    std::unordered_map<int64_t, int64_t> removeSess;
    removeSess.emplace(id, startTime);
    executor_->add([removeSess, this] () {
        this->doRemoveSession(removeSess);
    });

    return session;
}


int64_t SessionManager::newSessionId() {
    int64_t id = ++nextId_;
    if (id == 0) {
        id = ++nextId_;
    }
    return id;
}


// TODO(dutor) Now we do a brute-force scanning, of course we could make it more efficient.
void SessionManager::reclaimExpiredSessions() {
    folly::RWSpinLock::WriteHolder holder(rwlock_);
    if (FLAGS_session_idle_timeout_secs == 0) {
        metaClient_->pushSessionToCache(graphServerAddr_, activeSessForMeta_);
        return;
    }

    if (activeSessions_.empty()) {
        return;
    }

    FVLOG3("Try to reclaim expired sessions out of %lu ones", activeSessions_.size());
    auto iter = activeSessions_.begin();
    auto end = activeSessions_.end();
    while (iter != end) {
        auto *session = iter->second.first.get();
        int32_t idleSecs = session->idleSeconds();
        if (idleSecs < FLAGS_session_idle_timeout_secs) {
            ++iter;
            continue;
        }
        FLOG_INFO("Session %ld has expired", session->id());
        activeSessForMeta_.erase(iter->first);
        iter = activeSessions_.erase(iter);
    }

    metaClient_->pushSessionToCache(graphServerAddr_, activeSessForMeta_);
}

}   // namespace graph
}   // namespace nebula
