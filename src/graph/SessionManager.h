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

/**
 * SessionManager manages the client sessions, e.g. create new, find existing and drop expired.
 */

namespace nebula {
namespace graph {

class SessionManager final {
public:
    SessionManager();
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
     * Remove a session
     */
    SessionPtr removeSession(int64_t id);

private:
    /**
     * Generate a non-zero number
     */
    int64_t newSessionId();

    void reclaimExpiredSessions();

private:
    std::atomic<int64_t>                        nextId_{0};
    folly::RWSpinLock                           rwlock_;        // TODO(dutor) writer might starve
    std::unordered_map<int64_t, SessionPtr>     activeSessions_;
    std::unique_ptr<thread::GenericWorker>      scavenger_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_SESSIONMANAGER_H_
