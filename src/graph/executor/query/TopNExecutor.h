// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_TOPNEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_TOPNEXECUTOR_H_

#include "graph/executor/Executor.h"
namespace nebula {
namespace graph {

class TopNExecutor final : public Executor {
 public:
  TopNExecutor(const PlanNode *node, QueryContext *qctx) : Executor("TopNExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  template <typename U>
  void executeTopN(Iterator *iter);

  int64_t offset_;
  int64_t maxCount_;
  int64_t heapSize_;
  std::function<bool(const Row &, const Row &)> comparator_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_TOPNEXECUTOR_H_
