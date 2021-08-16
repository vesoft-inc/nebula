/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTOR_QUERY_AGGREGATEEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_AGGREGATEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class AggregateExecutor final : public Executor {
 public:
  AggregateExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("AggregateExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_AGGREGATEEXECUTOR_H_
