/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_LOGIC_ARGUMENTEXECUTOR_H
#define GRAPH_EXECUTOR_LOGIC_ARGUMENTEXECUTOR_H

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {
class ArgumentExecutor final : public Executor {
 public:
  ArgumentExecutor(const PlanNode *node, QueryContext *qctx);

  folly::Future<Status> execute() override;

 private:
};

}  // namespace graph
}  // namespace nebula
#endif
