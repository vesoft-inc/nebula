/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SHOW_SERVICE_CLIENTS_EXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SHOW_SERVICE_CLIENTS_EXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "graph/executor/Executor.h"  // for Executor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class PlanNode;
class QueryContext;

class ShowServiceClientsExecutor final : public Executor {
 public:
  ShowServiceClientsExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowServiceClientsExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> showServiceClients();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SHOW_SERVICE_CLIENTS_EXECUTOR_H_
