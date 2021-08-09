/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/interface/gen-cpp2/common_types.h"
#include "common/interface/gen-cpp2/meta_types.h"

#include "executor/admin/SessionExecutor.h"
#include "planner/plan/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowSessionsExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *showNode = asNode<ShowSessions>(node());
    if (showNode->isSetSessionID()) {
        return getSession(showNode->getSessionId());
    } else {
        return  listSessions();
    }
}

folly::Future<Status> ShowSessionsExecutor::listSessions() {
    return qctx()->getMetaClient()->listSessions()
            .via(runner())
            .thenValue([this](StatusOr<meta::cpp2::ListSessionsResp> resp) {
                SCOPED_TIMER(&execTime_);
                if (!resp.ok()) {
                    return Status::Error("Show sessions failed: %s.",
                                          resp.status().toString().c_str());
                }
                auto sessions = resp.value().get_sessions();
                DataSet result({"SessionId",
                                "UserName",
                                "SpaceName",
                                "CreateTime",
                                "UpdateTime",
                                "GraphAddr",
                                "Timezone",
                                "ClientIp"});
                for (auto &session : sessions) {
                    Row row;
                    row.emplace_back(session.get_session_id());
                    row.emplace_back(session.get_user_name());
                    row.emplace_back(session.get_space_name());
                    row.emplace_back(
                            microSecToDateTime(session.get_create_time()));
                    row.emplace_back(
                            microSecToDateTime(session.get_update_time()));
                    row.emplace_back(
                            network::NetworkUtils::toHostsStr({session.get_graph_addr()}));
                    row.emplace_back(session.get_timezone());
                    row.emplace_back(session.get_client_ip());
                    result.emplace_back(std::move(row));
                }
                return finish(ResultBuilder().value(Value(std::move(result))).finish());
            });
}

folly::Future<Status> ShowSessionsExecutor::getSession(SessionID sessionId) {
    return qctx()->getMetaClient()->getSession(sessionId)
            .via(runner())
            .thenValue([this, sessionId](StatusOr<meta::cpp2::GetSessionResp> resp) {
                SCOPED_TIMER(&execTime_);
                if (!resp.ok()) {
                    return Status::Error("Get session `%ld' failed: %s.",
                                          sessionId,
                                          resp.status().toString().c_str());
                }
                auto session = resp.value().get_session();
                DataSet result({"VariableName", "Value"});
                result.emplace_back(
                        Row({"SessionID", session.get_session_id()}));
                result.emplace_back(
                        Row({"UserName", session.get_user_name()}));
                result.emplace_back(
                        Row({"SpaceName", session.get_space_name()}));
                result.emplace_back(
                        Row({"CreateTime", microSecToDateTime(session.get_create_time())}));
                result.emplace_back(
                        Row({"UpdateTime", microSecToDateTime(session.get_update_time())}));
                result.emplace_back(
                        Row({"GraphAddr",
                             network::NetworkUtils::toHostsStr({session.get_graph_addr()})}));
                result.emplace_back(
                        Row({"Timezone", session.get_timezone()}));
                result.emplace_back(
                        Row({"ClientIp", session.get_client_ip()}));
                return finish(ResultBuilder().value(Value(std::move(result))).finish());
            });
}

folly::Future<Status> UpdateSessionExecutor::execute() {
    VLOG(1) << "Update sessions to metad";
    SCOPED_TIMER(&execTime_);
    auto *updateNode = asNode<UpdateSession>(node());
    std::vector<meta::cpp2::Session> sessions;
    sessions.emplace_back(updateNode->getSession());
    return qctx()->getMetaClient()->updateSessions(sessions)
            .via(runner())
            .thenValue([this](auto&& resp) {
                SCOPED_TIMER(&execTime_);
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
