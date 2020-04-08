/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SESSIONMANAGER_H_
#define GRAPH_SESSIONMANAGER_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "thread/GenericWorker.h"
#include "common/session/Session.h"
#include "meta/client/MetaClient.h"

/**
 * SessionManager manages the client sessions, e.g. create new, find existing and drop expired.
 */

namespace nebula {
namespace graph {

class SessionManager final {
public:
    explicit SessionManager(meta::MetaClient* client);
    ~SessionManager();

    using SessionPtr = std::shared_ptr<session::Session>;
    /**
     * Find an existing session
     */
    StatusOr<SessionPtr> findSession(int64_t id);
    /**
     * Create a new session
     */
    SessionPtr createSession();
    /**
     * Add a session to meta global session
     */
    void doAddSession(int64_t sid, int64_t startTime);
    /**
     * Remove a session
     */
    SessionPtr removeSession(int64_t id);
    /**
     * Remove a session from meta global session
     */
    void doRemoveSession(std::unordered_map<int64_t, int64_t> removeSess);

private:
    /**
     * Generate a non-zero number
     */
    int64_t newSessionId();

    void reclaimExpiredSessions();

private:
    std::atomic<int64_t>                                            nextId_{0};
    // TODO(dutor) writer might starve
    folly::RWSpinLock                                               rwlock_;
    std::unordered_map<int64_t, std::pair<SessionPtr, int64_t>>     activeSessions_;
    std::unordered_map<int64_t, int64_t>                            activeSessForMeta_;
    std::unique_ptr<thread::GenericWorker>                          scavenger_;
    meta::MetaClient                                               *metaClient_{nullptr};
    std::unique_ptr<folly::Executor>                                executor_;
    std::unique_ptr<folly::Executor>                                runner_;
    std::string                                                     graphServerAddr_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_SESSIONMANAGER_H_
