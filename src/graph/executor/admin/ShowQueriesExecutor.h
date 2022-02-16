/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SHOWQUERIESEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SHOWQUERIESEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "graph/executor/Executor.h"  // for Executor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph
namespace meta {
namespace cpp2 {
class Session;
}  // namespace cpp2
}  // namespace meta
struct DataSet;

class Status;
namespace meta {
namespace cpp2 {
class Session;
}  // namespace cpp2
}  // namespace meta
struct DataSet;

namespace graph {
class PlanNode;
class QueryContext;

class ShowQueriesExecutor final : public Executor {
 public:
  ShowQueriesExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("ShowQueriesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  friend class ShowQueriesTest_TestAddQueryAndTopN_Test;
  folly::Future<Status> showCurrentSessionQueries();

  folly::Future<Status> showAllSessionQueries();

  void addQueries(const meta::cpp2::Session& session, DataSet& dataSet) const;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ADMIN_SHOWQUERIESEXECUTOR_H_
