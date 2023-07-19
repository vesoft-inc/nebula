// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_LISTENEREXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_LISTENEREXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class AddListenerExecutor final : public Executor {
 public:
  AddListenerExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("AddListenerExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class RemoveListenerExecutor final : public Executor {
 public:
  RemoveListenerExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("RemoveListenerExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowListenerExecutor final : public Executor {
 public:
  ShowListenerExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowListenerExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_LISTENEREXECUTOR_H_
