/* Copyright (c) 2021 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "executor/admin/ShowQueriesExecutor.h"

#include "common/time/TimeUtils.h"
#include "context/QueryContext.h"
#include "planner/plan/Admin.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> ShowQueriesExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto* showQueries = asNode<ShowQueries>(node());
    auto isAll = showQueries->isAll();

    if (!isAll) {
        return showCurrentSessionQueries();
    } else {
        return showAllSessionQueries();
    }
}

folly::Future<Status> ShowQueriesExecutor::showCurrentSessionQueries() {
    DataSet dataSet({"SessionID",
                     "ExecutionPlanID",
                     "User",
                     "Host",
                     "StartTime",
                     "DurationInUSec",
                     "Status",
                     "Query"});
    auto* session = qctx()->rctx()->session();
    auto sessionInMeta = session->getSession();

    addQueries(sessionInMeta, dataSet);
    return finish(ResultBuilder()
                      .value(Value(std::move(dataSet)))
                      .iter(Iterator::Kind::kSequential)
                      .finish());
}

// The queries might not sync to meta completely.
folly::Future<Status> ShowQueriesExecutor::showAllSessionQueries() {
    return qctx()->getMetaClient()->listSessions()
            .via(runner())
            .thenValue([this](StatusOr<meta::cpp2::ListSessionsResp> resp) {
                SCOPED_TIMER(&execTime_);
                if (!resp.ok()) {
                    return Status::Error("Show sessions failed: %s.",
                                          resp.status().toString().c_str());
                }
                auto sessions = resp.value().get_sessions();
                DataSet dataSet({"SessionID",
                                 "ExecutionPlanID",
                                 "User",
                                 "Host",
                                 "StartTime",
                                 "DurationInUSec",
                                 "Status",
                                 "Query"});
                for (auto& session : sessions) {
                    addQueries(session, dataSet);
                }
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kSequential)
                                  .finish());
        });
}

void ShowQueriesExecutor::addQueries(const meta::cpp2::Session& session, DataSet& dataSet) const {
    auto& queries = session.get_queries();
    for (auto& query : queries) {
        Row row;
        row.values.emplace_back(session.get_session_id());
        row.values.emplace_back(query.first);
        row.values.emplace_back(session.get_user_name());
        row.values.emplace_back(query.second.get_graph_addr().toString());
        auto dateTime =
            time::TimeConversion::unixSecondsToDateTime(query.second.get_start_time() / 1000000);
        dateTime.microsec = query.second.get_start_time() % 1000000;
        row.values.emplace_back(std::move(dateTime));
        row.values.emplace_back(query.second.get_duration());
        row.values.emplace_back(apache::thrift::util::enumNameSafe(query.second.get_status()));
        row.values.emplace_back(query.second.get_query());
        dataSet.rows.emplace_back(std::move(row));
    }
}

}  // namespace graph
}  // namespace nebula
