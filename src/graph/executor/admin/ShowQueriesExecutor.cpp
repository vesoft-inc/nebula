/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ShowQueriesExecutor.h"

#include <folly/Try.h>                      // for Try::~Try<T>
#include <folly/futures/Future.h>           // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <algorithm>      // for max
#include <string>         // for string, basic_string, all...
#include <unordered_map>  // for _Node_const_iterator, ope...
#include <utility>        // for move, pair
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Status.h"             // for Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for Row, DataSet
#include "common/datatypes/Date.h"          // for DateTime, DateTime::(anon...
#include "common/datatypes/HostAddr.h"      // for HostAddr
#include "common/datatypes/Value.h"         // for Value
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "common/time/TimeConversion.h"     // for TimeConversion
#include "graph/context/Iterator.h"         // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/context/Result.h"           // for ResultBuilder
#include "graph/planner/plan/Admin.h"       // for ShowQueries
#include "graph/service/RequestContext.h"   // for RequestContext
#include "graph/session/ClientSession.h"    // for ClientSession
#include "interface/gen-cpp2/meta_types.h"  // for QueryDesc, ListSessionsResp

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
  return finish(
      ResultBuilder().value(Value(std::move(dataSet))).iter(Iterator::Kind::kSequential).build());
}

// The queries might not sync to meta completely.
folly::Future<Status> ShowQueriesExecutor::showAllSessionQueries() {
  return qctx()->getMetaClient()->listSessions().via(runner()).thenValue(
      [this](StatusOr<meta::cpp2::ListSessionsResp> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          return Status::Error("Show sessions failed: %s.", resp.status().toString().c_str());
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
                          .build());
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
