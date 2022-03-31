// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_AGGREGATEEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_AGGREGATEEXECUTOR_H_

#include "graph/executor/Executor.h"
// calculate a set of data uniformly. use values ​​from multiple records as input
// and convert those values ​​into one value to aggregate all records
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
