/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_KILLQUERYEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_KILLQUERYEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <vector>         // for allocator, vector

#include "common/base/Status.h"         // for Status
#include "common/thrift/ThriftTypes.h"  // for ExecutionPlanID, SessionID
#include "graph/executor/Executor.h"    // for Executor

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

namespace meta {
namespace cpp2 {
class Session;

class Session;
}  // namespace cpp2
}  // namespace meta

namespace graph {
class PlanNode;
class QueryContext;

class KillQueryExecutor final : public Executor {
 public:
  KillQueryExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("KillQueryExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  using QueriesMap = std::unordered_map<SessionID, std::unordered_set<ExecutionPlanID>>;
  Status verifyTheQueriesByLocalCache(QueriesMap& toBeVerifiedQueries, QueriesMap& killQueries);

  Status verifyTheQueriesByMetaInfo(const QueriesMap& toBeVerifiedQueries,
                                    const std::vector<meta::cpp2::Session>& sessionsInMeta);

  void killCurrentHostQueries(const QueriesMap& killQueries, QueriesMap& queriesKilledInLocal);

  void findKillQueriesViaMeta(const QueriesMap& queriesKilledInLocal,
                              const std::vector<meta::cpp2::Session>& sessionsInMeta,
                              QueriesMap& killQueries);
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_KILLQUERYEXECUTOR_H_
