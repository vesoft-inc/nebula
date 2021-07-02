/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SESSION_GRAPHSESSIONMANAGER_H_
#define SESSION_GRAPHSESSIONMANAGER_H_

#include "common/session/SessionManager.h"
#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "common/base/StatusOr.h"
#include "common/thread/GenericWorker.h"
#include "common/clients/meta/MetaClient.h"
#include "common/interface/gen-cpp2/GraphService.h"
#include "session/ClientSession.h"

/**
 * GraphSessionManager manages the client sessions, e.g. create new, find existing and drop expired.
 */

DECLARE_int64(max_allowed_connections);

namespace nebula {
namespace graph {
class GraphSessionManager final : public SessionManager<ClientSession> {
public:
    GraphSessionManager(meta::MetaClient* metaClient, const HostAddr &hostAddr);
    ~GraphSessionManager() {}

    Status init();

    /**
     * Create a new session
     */
    folly::Future<StatusOr<std::shared_ptr<ClientSession>>>
    createSession(const std::string userName,
                  const std::string clientIp,
                  folly::Executor* runner) override;

    bool isOutOfConnections() {
        if (activeSessions_.size() >= static_cast<uint64_t>(FLAGS_max_allowed_connections)) {
            LOG(INFO) << "The sessions of the cluster has more than max_allowed_connections: "
                      << FLAGS_max_allowed_connections;
            return false;
        }
        return true;
    }

    /**
     * Remove a session
     */
    void removeSession(SessionID id) override;

    folly::Future<StatusOr<std::shared_ptr<ClientSession>>>
    findSession(SessionID id, folly::Executor* runner) override;

    /**
     * Find an existing session
     */
    std::shared_ptr<ClientSession> findSessionFromCache(SessionID id);

private:
    folly::Future<StatusOr<std::shared_ptr<ClientSession>>>
    findSessionFromMetad(SessionID id, folly::Executor* runner);

    void threadFunc();

    void reclaimExpiredSessions();

    void updateSessionsToMeta();

    void updateSessionInfo(ClientSession* session);
};

}   // namespace graph
}   // namespace nebula


#endif  // SESSION_GRAPHSESSIONMANAGER_H_
