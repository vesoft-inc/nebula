/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_INGESTEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_INGESTEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class IngestExecutor final : public Executor {
 public:
  IngestExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("IngestExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_INGESTEXECUTOR_H_
