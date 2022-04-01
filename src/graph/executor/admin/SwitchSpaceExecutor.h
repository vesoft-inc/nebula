// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_SWITCHSPACEEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_SWITCHSPACEEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class SwitchSpaceExecutor final : public Executor {
 public:
  SwitchSpaceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SwitchSpaceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_SWITCHSPACEEXECUTOR_H_
