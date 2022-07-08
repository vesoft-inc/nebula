// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_FILTEREXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_FILTEREXECUTOR_H_

#include "graph/executor/Executor.h"

// delete the corresponding iterator when the row in the dataset does not meet the conditions
// and save the filtered iterator to the result
namespace nebula {
namespace graph {

class FilterExecutor final : public Executor {
 public:
  FilterExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("FilterExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

  StatusOr<DataSet> handleJob(size_t begin, size_t end, Iterator *iter);

  Status handleSingleJobFilter();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_FILTEREXECUTOR_H_
