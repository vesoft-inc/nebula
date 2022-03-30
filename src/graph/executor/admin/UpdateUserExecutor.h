// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_UPDATEUSEREXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_UPDATEUSEREXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class UpdateUserExecutor final : public Executor {
 public:
  UpdateUserExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("UpdateUserExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> updateUser();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_UPDATEUSEREXECUTOR_H_
