// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {
class BFSShortestPathExecutor final : public Executor {
 public:
  BFSShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("BFSShortestPath", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  void buildPath(Iterator* iter, bool reverse);

  std::vector<Row> conjunctPath();

  std::unordered_multimap<Value, Path> createPath(std::vector<Value> meetVids,
                                                  bool reverse,
                                                  bool oddStep);

 private:
  size_t step_{1};
  size_t steps_{1};
  std::string leftVidVar_;
  std::string rightVidVar_;
  std::unordered_set<Value> leftVisitedVids_;
  std::unordered_set<Value> rightVisitedVids_;
  std::vector<std::unordered_multimap<Value, Edge>> allLeftEdges_;
  std::vector<std::unordered_multimap<Value, Edge>> allRightEdges_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
