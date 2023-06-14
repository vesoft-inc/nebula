// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_SAMPLINGEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SAMPLINGEXECUTOR_H_

#include "graph/executor/Executor.h"
namespace nebula {
namespace graph {

class SamplingExecutor final : public Executor {
 public:
  SamplingExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SamplingExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  template <typename U>
  void executeBinarySample(Iterator *iter, size_t index, size_t count, DataSet &list);
  template <typename U>
  void executeAliasSample(Iterator *iter, size_t index, size_t count, DataSet &list);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_QUERY_SAMPLINGEXECUTOR_H_
