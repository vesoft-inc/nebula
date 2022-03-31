// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_UNIONALLVERSIONVAREXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_UNIONALLVERSIONVAREXECUTOR_H_

#include "graph/executor/Executor.h"
// leftSet + rightSet. column names must be the same
namespace nebula {
namespace graph {

class UnionAllVersionVarExecutor : public Executor {
 public:
  UnionAllVersionVarExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("UnionAllVersionVarExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_UNIONALLVERSIONVAREXECUTOR_H_
