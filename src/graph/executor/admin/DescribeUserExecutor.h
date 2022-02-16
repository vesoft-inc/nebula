/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_DESCRIBEUSEREXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_DESCRIBEUSEREXECUTOR_H_

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

class DescribeUserExecutor final : public Executor {
 public:
  DescribeUserExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DescribeUsersExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> describeUser();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_LISTUSERSEXECUTOR_H_
