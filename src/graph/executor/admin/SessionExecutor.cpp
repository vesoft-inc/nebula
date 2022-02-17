/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/SessionExecutor.h"

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future::Future<T>
#include <folly/futures/Promise.h>  // for PromiseException::Pro...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException::Pro...

#include <algorithm>    // for max
#include <string>       // for string, basic_string
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "clients/meta/MetaClient.h"            // for MetaClient
#include "common/base/Logging.h"                // for LogMessage, COMPACT_G...
#include "common/base/Status.h"                 // for Status, operator<<
#include "common/base/StatusOr.h"               // for StatusOr
#include "common/datatypes/DataSet.h"           // for Row, DataSet
#include "common/datatypes/HostAddr.h"          // for HostAddr
#include "common/datatypes/Value.h"             // for Value
#include "common/network/NetworkUtils.h"        // for NetworkUtils
#include "common/time/ScopedTimer.h"            // for SCOPED_TIMER
#include "graph/context/QueryContext.h"         // for QueryContext
#include "graph/context/Result.h"               // for ResultBuilder
#include "graph/planner/plan/Admin.h"           // for ShowSessions, UpdateS...
#include "graph/service/RequestContext.h"       // for RequestContext
#include "graph/session/GraphSessionManager.h"  // for GraphSessionManager
#include "interface/gen-cpp2/meta_types.h"      // for Session, GetSessionResp

namespace nebula {
namespace graph {

folly::Future<Status> ShowSessionsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *showNode = asNode<ShowSessions>(node());
  if (showNode->isSetSessionID()) {
    return getSession(showNode->getSessionId());
  }
  return showNode->isLocalCommand() ? listLocalSessions() : listSessions();
}

folly::Future<Status> ShowSessionsExecutor::listSessions() {
  return qctx()->getMetaClient()->listSessions().via(runner()).thenValue(
      [this](StatusOr<meta::cpp2::ListSessionsResp> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return Status::Error("Show sessions failed: %s.", resp.status().toString().c_str());
        }
        auto sessions = resp.value().get_sessions();
        // Construct result column names
        DataSet result({"SessionId",
                        "UserName",
                        "SpaceName",
                        "CreateTime",
                        "UpdateTime",
                        "GraphAddr",
                        "Timezone",
                        "ClientIp"});
        for (auto &session : sessions) {
          addSessions(session, result);
        }
        return finish(ResultBuilder().value(Value(std::move(result))).build());
      });
}

folly::Future<Status> ShowSessionsExecutor::listLocalSessions() {
  auto localSessions = qctx_->rctx()->sessionMgr()->getSessionFromLocalCache();
  DataSet result({"SessionId",
                  "UserName",
                  "SpaceName",
                  "CreateTime",
                  "UpdateTime",
                  "GraphAddr",
                  "Timezone",
                  "ClientIp"});
  for (auto &session : localSessions) {
    addSessions(session, result);
  }
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

folly::Future<Status> ShowSessionsExecutor::getSession(SessionID sessionId) {
  return qctx()->getMetaClient()->getSession(sessionId).via(runner()).thenValue(
      [this, sessionId](StatusOr<meta::cpp2::GetSessionResp> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return Status::Error(
              "Get session `%ld' failed: %s.", sessionId, resp.status().toString().c_str());
        }
        auto session = resp.value().get_session();

        // Construct result column names
        DataSet result({"SessionId",
                        "UserName",
                        "SpaceName",
                        "CreateTime",
                        "UpdateTime",
                        "GraphAddr",
                        "Timezone",
                        "ClientIp"});
        addSessions(session, result);
        return finish(ResultBuilder().value(Value(std::move(result))).build());
      });
}

void ShowSessionsExecutor::addSessions(const meta::cpp2::Session &session, DataSet &dataSet) const {
  Row row;
  row.emplace_back(session.get_session_id());
  row.emplace_back(session.get_user_name());
  row.emplace_back(session.get_space_name());
  row.emplace_back(microSecToDateTime(session.get_create_time()));
  row.emplace_back(microSecToDateTime(session.get_update_time()));
  row.emplace_back(network::NetworkUtils::toHostsStr({session.get_graph_addr()}));
  row.emplace_back(session.get_timezone());
  row.emplace_back(session.get_client_ip());
  dataSet.emplace_back(std::move(row));
}

folly::Future<Status> UpdateSessionExecutor::execute() {
  VLOG(1) << "Update sessions to metad";
  SCOPED_TIMER(&execTime_);
  auto *updateNode = asNode<UpdateSession>(node());
  std::vector<meta::cpp2::Session> sessions;
  sessions.emplace_back(updateNode->getSession());
  return qctx()->getMetaClient()->updateSessions(sessions).via(runner()).thenValue(
      [this](auto &&resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
