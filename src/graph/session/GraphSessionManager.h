/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SESSION_GRAPHSESSIONMANAGER_H_
#define GRAPH_SESSION_GRAPHSESSIONMANAGER_H_

#include <folly/concurrency/ConcurrentHashMap.h>  // for ConcurrentHashMap
#include <folly/futures/Future.h>                 // for Future
#include <gflags/gflags_declare.h>                // for DECLARE_int64
#include <stdint.h>                               // for uint64_t

#include <memory>         // for shared_ptr
#include <string>         // for string
#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/base/Logging.h"  // for LOG, LogMessage
#include "common/base/Status.h"   // for Status
#include "common/base/StatusOr.h"
#include "common/session/SessionManager.h"  // for SessionManager
#include "common/thread/GenericWorker.h"
#include "common/thrift/ThriftTypes.h"  // for SessionID
#include "graph/session/ClientSession.h"
#include "interface/gen-cpp2/GraphService.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {
class ClientSession;
}  // namespace graph
namespace meta {
class MetaClient;
namespace cpp2 {
class Session;
}  // namespace cpp2
}  // namespace meta
struct HostAddr;
template <typename T>
class StatusOr;
}  // namespace nebula

namespace folly {
class Executor;

class Executor;
}  // namespace folly
namespace nebula {
namespace graph {
class ClientSession;
}  // namespace graph
namespace meta {
class MetaClient;
namespace cpp2 {
class Session;
}  // namespace cpp2
}  // namespace meta
struct HostAddr;
template <typename T>
class StatusOr;
}  // namespace nebula

/**
 * GraphSessionManager manages the client sessions, e.g. create new, find
 * existing and drop expired.
 */

DECLARE_int64(max_allowed_connections);

namespace nebula {
namespace graph {
class GraphSessionManager final : public SessionManager<ClientSession> {
 public:
  GraphSessionManager(meta::MetaClient* metaClient, const HostAddr& hostAddr);
  ~GraphSessionManager() {}

  Status init();

  /**
   * Create a new session
   */
  folly::Future<StatusOr<std::shared_ptr<ClientSession>>> createSession(
      const std::string userName, const std::string clientIp, folly::Executor* runner) override;

  bool isOutOfConnections() {
    if (activeSessions_.size() >= static_cast<uint64_t>(FLAGS_max_allowed_connections)) {
      LOG(INFO) << "The sessions of the cluster has more than "
                   "max_allowed_connections: "
                << FLAGS_max_allowed_connections;
      return false;
    }
    return true;
  }

  /**
   * Remove a session
   */
  void removeSession(SessionID id) override;

  folly::Future<StatusOr<std::shared_ptr<ClientSession>>> findSession(
      SessionID id, folly::Executor* runner) override;

  /**
   * Find an existing session
   */
  std::shared_ptr<ClientSession> findSessionFromCache(SessionID id);

  // get all seesions in the local cache
  std::vector<meta::cpp2::Session> getSessionFromLocalCache() const;

 private:
  folly::Future<StatusOr<std::shared_ptr<ClientSession>>> findSessionFromMetad(
      SessionID id, folly::Executor* runner);

  void threadFunc();

  void reclaimExpiredSessions();

  void updateSessionsToMeta();

  void updateSessionInfo(ClientSession* session);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_SESSION_GRAPHSESSIONMANAGER_H_
