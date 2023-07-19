// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/KillQueryExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {
folly::Future<Status> KillQueryExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  QueriesMap toBeVerifiedQueries;
  QueriesMap killQueries;
  NG_RETURN_IF_ERROR(verifyTheQueriesByLocalCache(toBeVerifiedQueries, killQueries));

  return qctx()
      ->getMetaClient()
      ->listSessions()
      .via(runner())
      .thenValue([toBeVerifiedQueries = std::move(toBeVerifiedQueries),
                  killQueries = std::move(killQueries),
                  this](StatusOr<meta::cpp2::ListSessionsResp> listResp) mutable {
        std::vector<meta::cpp2::Session> sessionsInMeta;
        if (listResp.ok()) {
          sessionsInMeta = std::move(listResp.value()).get_sessions();
        } else {
          LOG(WARNING) << "List session fail: " << listResp.status();
        }

        auto status = verifyTheQueriesByMetaInfo(toBeVerifiedQueries, sessionsInMeta);
        if (!status.ok()) {
          return folly::makeFuture<StatusOr<meta::cpp2::ExecResp>>(status);
        }

        QueriesMap queriesKilledInLocal;
        killCurrentHostQueries(killQueries, queriesKilledInLocal);
        findKillQueriesViaMeta(queriesKilledInLocal, sessionsInMeta, killQueries);

        // upload queries to be killed to meta.
        return qctx()->getMetaClient()->killQuery(std::move(killQueries));
      })
      .thenValue([](auto&& resp) {
        if (!resp.ok()) {
          return resp.status();
        }
        return Status::OK();
      });
}

Status KillQueryExecutor::verifyTheQueriesByLocalCache(QueriesMap& toBeVerifiedQueries,
                                                       QueriesMap& killQueries) {
  auto* killQuery = asNode<KillQuery>(node());
  auto inputVar = killQuery->inputVar();
  auto iter = ectx_->getResult(inputVar).iter();
  DCHECK(!!iter);
  QueryExpressionContext ctx(ectx_);
  auto sessionExpr = killQuery->sessionId();
  auto epExpr = killQuery->epId();

  auto* session = qctx()->rctx()->session();
  auto* sessionMgr = qctx_->rctx()->sessionMgr();
  for (; iter->valid(); iter->next()) {
    auto& sessionVal = sessionExpr->eval(ctx(iter.get()));
    if (!sessionVal.isInt()) {
      std::stringstream ss;
      ss << "Session `" << sessionExpr->toString() << "' is not kind of"
         << " int, but was " << sessionVal.type();
      return Status::Error(ss.str());
    }
    auto& epVal = epExpr->eval(ctx(iter.get()));
    if (!epVal.isInt()) {
      std::stringstream ss;
      ss << "ExecutionPlanID `" << epExpr->toString() << "' is not kind of"
         << " int, but was " << epVal.type();
      return Status::Error(ss.str());
    }

    auto sessionId = sessionVal.getInt();
    auto epId = epVal.getInt();
    if (sessionId == session->id() || sessionId < 0) {
      if (!session->findQuery(epId)) {
        return Status::Error("ExecutionPlanId[%ld] does not exist in current Session.", epId);
      }
      killQueries[session->id()].emplace(epId);
    } else {
      auto sessionPtr = sessionMgr->findSessionFromCache(sessionId);
      if (sessionPtr == nullptr) {
        toBeVerifiedQueries[sessionId].emplace(epId);
      } else {  // Sessions in current host
        // Only GOD role could kill others' queries.
        if (sessionPtr->user() != session->user() && !session->isGod()) {
          return Status::PermissionError("Only GOD role could kill others' queries.");
        }
        if (!sessionPtr->findQuery(epId)) {
          return Status::Error(
              "ExecutionPlanId[%ld] does not exist in Session[%ld].", epId, sessionId);
        }
      }
      killQueries[sessionId].emplace(epId);
    }
  }
  return Status::OK();
}

void KillQueryExecutor::killCurrentHostQueries(const QueriesMap& killQueries,
                                               QueriesMap& queriesKilledInLocal) {
  auto* session = qctx()->rctx()->session();
  auto* sessionMgr = qctx_->rctx()->sessionMgr();
  for (auto& s : killQueries) {
    auto sessionId = s.first;
    for (auto& epId : s.second) {
      if (sessionId == session->id()) {
        session->markQueryKilled(epId);
        queriesKilledInLocal[sessionId].emplace(epId);
      } else {
        auto sessionPtr = sessionMgr->findSessionFromCache(sessionId);
        if (sessionPtr != nullptr) {
          sessionPtr->markQueryKilled(epId);
          queriesKilledInLocal[sessionId].emplace(epId);
        }
      }
    }
  }
}

Status KillQueryExecutor::verifyTheQueriesByMetaInfo(
    const QueriesMap& toBeVerifiedQueries, const std::vector<meta::cpp2::Session>& sessionsInMeta) {
  auto* currentSession = qctx()->rctx()->session();
  for (auto& s : toBeVerifiedQueries) {
    auto sessionId = s.first;
    auto found = std::find_if(sessionsInMeta.begin(), sessionsInMeta.end(), [sessionId](auto& val) {
      return val.get_session_id() == sessionId;
    });
    if (found == sessionsInMeta.end()) {
      return Status::Error("SessionId[%ld] does not exist", sessionId);
    }
    for (auto& epId : s.second) {
      if (found->get_queries().find(epId) == found->get_queries().end()) {
        return Status::Error(
            "ExecutionPlanId[%ld] does not exist in Session[%ld].", epId, sessionId);
      }
    }
    // Only GOD role could kill others' queries.
    if (found->get_user_name() != currentSession->user() && !currentSession->isGod()) {
      return Status::PermissionError("Only GOD role could kill others' queries.");
    }
  }
  return Status::OK();
}

void KillQueryExecutor::findKillQueriesViaMeta(
    const QueriesMap& queriesKilledInLocal,
    const std::vector<meta::cpp2::Session>& sessionsInMeta,
    QueriesMap& killQueries) {
  for (auto& kv : queriesKilledInLocal) {
    auto sessionId = kv.first;
    auto found = std::find_if(sessionsInMeta.begin(), sessionsInMeta.end(), [sessionId](auto& val) {
      return val.get_session_id() == sessionId;
    });
    DCHECK(found != sessionsInMeta.end());
    for (auto& epId : kv.second) {
      if (found->get_queries().find(epId) == found->get_queries().end()) {
        // Remove the queries that do not exists in meta since they have not
        // uploaded
        auto& queries = killQueries[sessionId];
        queries.erase(epId);
        if (queries.empty()) {
          killQueries.erase(sessionId);
        }
      }
    }
  }
}
}  // namespace graph
}  // namespace nebula
