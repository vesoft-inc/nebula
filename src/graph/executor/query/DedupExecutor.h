// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_DEDUPEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_DEDUPEXECUTOR_H_

#include "graph/executor/Executor.h"
// delete the corresponding iterator, when there are duplicate rows in the dataset.
// and then save the filtered iterator to the result
namespace nebula {
namespace graph {

class DedupExecutor final : public Executor {
 public:
  DedupExecutor(const PlanNode *node, QueryContext *qctx) : Executor("DedupExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_DEDUPEXECUTOR_H_
