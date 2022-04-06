// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_QUERY_LIMITEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_LIMITEXECUTOR_H_

#include "graph/executor/Executor.h"
// takes iterators of data with user-specified limits and save them to the result
namespace nebula {
namespace graph {

class LimitExecutor final : public Executor {
 public:
  LimitExecutor(const PlanNode *node, QueryContext *qctx) : Executor("LimitExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_LIMITEXECUTOR_H_
