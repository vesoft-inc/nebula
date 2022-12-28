// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/SessionExecutor.h"

#include "graph/planner/plan/Admin.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/meta_types.h"

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

folly::Future<Status> KillSessionExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *killNode = asNode<KillSession>(node());
  auto inputVar = killNode->inputVar();
  auto iter = ectx_->getResult(inputVar).iter();
  QueryExpressionContext ctx(ectx_);
  auto sessionExpr = killNode->getSessionId();

  // Collect all session ids
  std::vector<SessionID> sessionIds;
  for (; iter->valid(); iter->next()) {
    auto &sessionVal = sessionExpr->eval(ctx(iter.get()));
    if (!sessionVal.isInt()) {
      std::stringstream ss;
      ss << "Session `" << sessionExpr->toString() << "' is not kind of"
         << " int, but was " << sessionVal.type();
      return Status::Error(ss.str());
    }

    auto sessionId = sessionVal.getInt();
    sessionIds.emplace_back(sessionId);
  }

  auto sessionMgr = qctx_->rctx()->sessionMgr();
  auto killedSessionNum = sessionMgr->removeMultiSessions(sessionIds);

  // Construct result column names
  DataSet result({
      "Number of session killed",
  });
  Row row;
  row.emplace_back(killedSessionNum);
  result.emplace_back(std::move(row));
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

folly::Future<Status> UpdateSessionExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *updateNode = asNode<UpdateSession>(node());
  std::vector<meta::cpp2::Session> sessions;
  sessions.emplace_back(updateNode->getSession());
  return qctx()->getMetaClient()->updateSessions(sessions).via(runner()).thenValue(
      [this](auto &&resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(WARNING) << "Update session fail " << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
