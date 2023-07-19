// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_LOGIC_SELECTEXECUTOR_H_
#define GRAPH_EXECUTOR_LOGIC_SELECTEXECUTOR_H_

#include "graph/executor/Executor.h"
// like IF ELSE logic in c language
// when the condition_ is true, execute the then operator, otherwise execute the else operator
namespace nebula {
namespace graph {

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

  bool condition() const {
    return condition_;
  }

 private:
  Executor* then_;
  Executor* else_;

  // mark condition value
  bool condition_{false};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_LOGIC_SELECTEXECUTOR_H_
