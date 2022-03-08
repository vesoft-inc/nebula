// Copyright (c) 2018 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_SESSION_GRAPHSESSIONMANAGER_H_
#define GRAPH_SESSION_GRAPHSESSIONMANAGER_H_

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/session/SessionManager.h"
#include "common/thread/GenericWorker.h"
#include "common/thrift/ThriftTypes.h"
#include "graph/session/ClientSession.h"
#include "interface/gen-cpp2/GraphService.h"
#include "interface/gen-cpp2/meta_types.h"

DECLARE_int64(max_allowed_connections);

namespace nebula {
namespace graph {

// GraphSessionManager manages the client sessions, e.g. create new, find existing and drop expired.
// Nebula's session management strategy:
// When a user requests the graph server to create a session, the graph server forwards the request
// to the meta server, which allocates the session and returns it to the graph server.
// The meta server manages all the sessions from all graph servers. One graph server only manages
// its own sessions, including periodically reclaiming expired sessions, and updating sessions
// information to the meta server in time. When the graph server restarts, it will pull all the
// sessions from the meta server and choose its own sessions for management.
class GraphSessionManager final : public SessionManager<ClientSession> {
 public:
  // Periodically reclaims expired sessions and updates information to the meta server.
  // in the background.
  // metaClient: The client of the meta server.
  // hostAddr: The address of the current graph server.
  GraphSessionManager(meta::MetaClient* metaClient, const HostAddr& hostAddr);
  ~GraphSessionManager() {}

  // Pulls sessions from the meta server and chooses its own sessions for management.
  Status init();

  // Creates a new session.
  // userName: The name of the user who requesting to create the session.
  // clientIp: The address of the client which sends the request.
  // runner: Ensure that the corresponding callback function is executed on the runner.
  // return: ClientSession which will be created.
  folly::Future<StatusOr<std::shared_ptr<ClientSession>>> createSession(
      const std::string userName, const std::string clientIp, folly::Executor* runner) override;

  // Whether exceeds the max allowed connections.
  bool isOutOfConnections() {
    if (activeSessions_.size() >= static_cast<uint64_t>(FLAGS_max_allowed_connections)) {
      LOG(INFO) << "The sessions of the cluster has more than "
                   "max_allowed_connections: "
                << FLAGS_max_allowed_connections;
      return true;
    }
    return false;
  }

  // Removes a session from both local and meta server.
  // id: The id of the session which will be removed.
  void removeSession(SessionID id) override;

  // Finds an existing session. If it is not found locally, it will be searched from the meta
  // server. id: The id of the session which will be found.
  // runner: Ensure that the corresponding callback function is executed on the runner.
  // return: ClientSession which will be found.
  folly::Future<StatusOr<std::shared_ptr<ClientSession>>> findSession(
      SessionID id, folly::Executor* runner) override;

  // Finds an existing session only from local cache.
  // id: The id of the session which will be found.
  // return: ClientSession which will be found.
  std::shared_ptr<ClientSession> findSessionFromCache(SessionID id);

  // Gets all seesions from the local cache.
  // return: All sessions of the local cache.
  std::vector<meta::cpp2::Session> getSessionFromLocalCache() const;

 private:
  // Finds an existing session only from the meta server.
  // id: The id of the session which will be found.
  // runner: Ensure that the corresponding callback function is executed on the runner.
  // return: ClientSession which will be found.
  folly::Future<StatusOr<std::shared_ptr<ClientSession>>> findSessionFromMetad(
      SessionID id, folly::Executor* runner);

  // Entry function of the background thread.
  // It will reclaim expired sessions and update sessions info to meta.
  void threadFunc();

  // Reclaims expired sessions.
  // All queries within the expired session will be marked as killed.
  void reclaimExpiredSessions();

  // Updates sessions into to meta server.
  void updateSessionsToMeta();

  // Updates session info locally.
  // session: ClientSession which will be updated.
  void updateSessionInfo(ClientSession* session);

  // add sessionCount
  bool addSessionCount(std::string& key);

  // sub sessionCount
  bool subSessionCount(std::string& key);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_SESSION_GRAPHSESSIONMANAGER_H_
