/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_LOGIC_SELECTEXECUTOR_H_
#define GRAPH_EXECUTOR_LOGIC_SELECTEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include "common/base/Logging.h"      // for CheckNotNull, DCHECK_NOTNULL
#include "graph/executor/Executor.h"  // for Executor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class PlanNode;
class QueryContext;

class SelectExecutor final : public Executor {
 public:
  SelectExecutor(const PlanNode* node, QueryContext* qctx);

  folly::Future<Status> execute() override;

  void setThenBody(Executor* then) {
    then_ = DCHECK_NOTNULL(then);
  }

  void setElseBody(Executor* els) {
    else_ = DCHECK_NOTNULL(els);
  }

  Executor* thenBody() const {
    return then_;
  }

  Executor* elseBody() const {
    return else_;
  }

 private:
  Executor* then_;
  Executor* else_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_LOGIC_SELECTEXECUTOR_H_
