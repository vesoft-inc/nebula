// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_SHOWMETALEADEREXECUTOR_H
#define GRAPH_EXECUTOR_ADMIN_SHOWMETALEADEREXECUTOR_H

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ShowMetaLeaderExecutor final : public Executor {
 public:
  ShowMetaLeaderExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowMetaLeaderExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula
#endif
