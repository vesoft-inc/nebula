/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_LOGIC_LOOPEXECUTOR_H_
#define GRAPH_EXECUTOR_LOGIC_LOOPEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class LoopExecutor final : public Executor {
 public:
  LoopExecutor(const PlanNode *node, QueryContext *qctx);

  folly::Future<Status> execute() override;

  void setLoopBody(Executor *body) {
    body_ = DCHECK_NOTNULL(body);
  }

  Executor *loopBody() const {
    return body_;
  }

  bool finally() const {
    return finally_;
  }

 private:
  // Hold the last executor node of loop body executors chain
  Executor *body_{nullptr};

  // mark will loop again
  bool finally_{false};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_LOGIC_LOOPEXECUTOR_H_
