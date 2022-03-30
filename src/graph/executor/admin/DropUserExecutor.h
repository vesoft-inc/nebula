// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_DROPUSEREXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_DROPUSEREXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class DropUserExecutor final : public Executor {
 public:
  DropUserExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DropUserExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> dropUser();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_DROPUSEREXECUTOR_H_
