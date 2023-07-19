// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_SAMPLEEXECUTOR_H
#define GRAPH_EXECUTOR_QUERY_SAMPLEEXECUTOR_H

#include "graph/executor/Executor.h"
// invoke the sampling algorithm to select the specified amount of data
namespace nebula {
namespace graph {

class SampleExecutor final : public Executor {
 public:
  SampleExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SampleExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula
#endif
