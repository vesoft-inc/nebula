//
// Created by fujie on 24-10-21.
//

#ifndef NEBULA_GRAPH_TRANSFERLEADEREXECUTOR_H
#define NEBULA_GRAPH_TRANSFERLEADEREXECUTOR_H

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class TransferLeaderExecutor final : public Executor {
 public:
  TransferLeaderExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("TransferLeaderExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> transferLeader();
};

}  // namespace graph
}  // namespace nebula

#endif  // NEBULA_GRAPH_TRANSFERLEADEREXECUTOR_H
