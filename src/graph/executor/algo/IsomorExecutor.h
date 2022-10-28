// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_ISOMOREXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_ISOMOREXECUTOR_H_

#include "graph/executor/Executor.h"
#include "graph/executor/subgraph_provenance/graph.h"
#include "graph/executor/subgraph_provenance/subgraph.h"

namespace nebula {
namespace graph {
class IsomorExecutor : public Executor {
 public:
  IsomorExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("IsomorExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  std::unordered_map<Value, int64_t> historyVids_;
  std::unique_ptr<Graph> generateGraph(Iterator* vIter, Iterator* eIter);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ALGO_ISOMOREXECUTOR_H_
