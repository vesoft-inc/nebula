/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_LOGIC_ARGUMENTEXECUTOR_H
#define GRAPH_EXECUTOR_LOGIC_ARGUMENTEXECUTOR_H

#include <folly/futures/Future.h>  // for Future

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

class ArgumentExecutor final : public Executor {
 public:
  ArgumentExecutor(const PlanNode *node, QueryContext *qctx);

  folly::Future<Status> execute() override;

 private:
};

}  // namespace graph
}  // namespace nebula
#endif
