/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_MINUSEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_MINUSEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "graph/executor/query/SetExecutor.h"  // for SetExecutor

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

class MinusExecutor : public SetExecutor {
 public:
  MinusExecutor(const PlanNode *node, QueryContext *qctx)
      : SetExecutor("MinusExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_MINUSEXECUTOR_H_
